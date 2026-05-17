package com.bux.ivp;

import com.bux.ivp.domain.PlanStatus;
import com.bux.ivp.repository.InvestmentPlanRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DeletePlanIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private InvestmentPlanRepository planRepository;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("DELETE FROM plan_investment");
        jdbcTemplate.execute("DELETE FROM investment_plan");
        jdbcTemplate.execute("DELETE FROM processed_message");

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));

        consumer.subscribe(List.of("ivp-events"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close();
        if (producer != null) producer.close();
    }

    private String createPlan(String commandId, String userId, String name) {
        return """
                {
                    "type": "CREATE_PLAN",
                    "commandId": "%s",
                    "userId": "%s",
                    "name": "%s",
                    "investments": [{"instrument": "AAPL", "amount": 100.00}],
                    "executionDay": 1
                }
                """.formatted(commandId, userId, name);
    }

    private String deletePlan(String commandId, String planId) {
        return """
                {
                    "type": "DELETE_PLAN",
                    "commandId": "%s",
                    "planId": "%s"
                }
                """.formatted(commandId, planId);
    }

    @Test
    void shouldDeletePlanAndPublishEvent() throws Exception {
        // create a plan first
        String createCommandId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String planName = "Plan to Delete " + UUID.randomUUID();

        Thread.sleep(2000);

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(createCommandId, userId, planName)));
        producer.flush();

        // wait for plan to be created
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName))
                        .toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        // now delete it
        String deleteCommandId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>("ivp-commands", deletePlan(deleteCommandId, planId)));
        producer.flush();

        // verify plan is deleted in DB
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var plan = planRepository.findById(UUID.fromString(planId));
            assertThat(plan).isPresent();
            assertThat(plan.get().getStatus()).isEqualTo(PlanStatus.DELETED);
        });

        // verify PlanDeleted event published
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var records = consumer.poll(Duration.ofSeconds(2));
            boolean deletedEventFound = false;
            for (var record : records) {
                var node = objectMapper.readTree(record.value());
                var planIdNode = node.get("planId");
                if (planIdNode != null && planId.equals(planIdNode.asText())) {
                    var typeNode = node.get("type");
                    if (typeNode != null && "PLAN_DELETED".equals(typeNode.asText())) {
                        deletedEventFound = true;
                        break;
                    }
                }
            }
            assertThat(deletedEventFound).isTrue();
        });
    }

    @Test
    void shouldIgnoreDuplicateDeleteCommand() throws Exception {
        String createCommandId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String planName = "Plan Duplicate Delete " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(createCommandId, userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName))
                        .toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        String deleteCommandId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>("ivp-commands", deletePlan(deleteCommandId, planId)));
        producer.send(new ProducerRecord<>("ivp-commands", deletePlan(deleteCommandId, planId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var plan = planRepository.findById(UUID.fromString(planId));
            assertThat(plan).isPresent();
            assertThat(plan.get().getStatus()).isEqualTo(PlanStatus.DELETED);
        });

        // only one processed_message record for this commandId
        int count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                Integer.class, deleteCommandId
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldNotDeleteAlreadyDeletedPlan() throws Exception {
        String createCommandId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String planName = "Already Deleted Plan " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(createCommandId, userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName))
                        .toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        // delete once
        producer.send(new ProducerRecord<>("ivp-commands", deletePlan(UUID.randomUUID().toString(), planId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var plan = planRepository.findById(UUID.fromString(planId));
            assertThat(plan.get().getStatus()).isEqualTo(PlanStatus.DELETED);
        });

        // delete again with different commandId
        producer.send(new ProducerRecord<>("ivp-commands", deletePlan(UUID.randomUUID().toString(), planId)));
        producer.flush();

        // plan should still be DELETED, not errored
        Thread.sleep(2000);
        var plan = planRepository.findById(UUID.fromString(planId));
        assertThat(plan).isPresent();
        assertThat(plan.get().getStatus()).isEqualTo(PlanStatus.DELETED);
    }
}