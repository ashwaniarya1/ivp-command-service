package com.bux.ivp;

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
class CreatePlanIntegrationTest {

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
    private ObjectMapper objectMapper;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> ivpEventConsumer;

    @AfterEach
    void tearDown() {
        if (ivpEventConsumer != null) {
            ivpEventConsumer.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

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

        ivpEventConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));

        ivpEventConsumer.subscribe(List.of("ivp-events"));
    }

    @Test
    void shouldCreatePlanAndPublishEvent() throws Exception {
        String commandId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        String uniqueName = "Tech Portfolio " + UUID.randomUUID();

        String message = """
            {
                "type": "CREATE_PLAN",
                "commandId": "%s",
                "userId": "%s",
                "name": "%s",
                "investments": [
                    {"instrument": "AAPL", "amount": 100.00},
                    {"instrument": "GOOGL", "amount": 50.00}
                ],
                "executionDay": 1
            }
            """.formatted(commandId, userId, uniqueName);

        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var records = ivpEventConsumer.poll(Duration.ofSeconds(2));
            boolean matchingEventReceived = false;
            for (var record : records) {
                var node = objectMapper.readTree(record.value());
                var nameNode = node.get("name");
                var typeNode = node.get("type");
                if (nameNode != null && uniqueName.equals(nameNode.asText())
                        && typeNode != null && "PLAN_CREATED".equals(typeNode.asText())) {
                    var investments = node.get("investments");
                    assertThat(investments).isNotNull();
                    assertThat(investments.isArray()).isTrue();
                    assertThat(investments.size()).isEqualTo(2);
                    var instruments = new java.util.ArrayList<String>();
                    investments.forEach(i -> instruments.add(i.get("instrument").asText()));
                    assertThat(instruments).containsExactlyInAnyOrder("AAPL", "GOOGL");
                    matchingEventReceived = true;
                    break;
                }
            }
            assertThat(matchingEventReceived).isTrue();
        });
    }

    @Test
    void shouldIgnoreDuplicateCommand() throws Exception {
        String commandId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();

        String message = """
                {
                    "type": "CREATE_PLAN",
                    "commandId": "%s",
                    "userId": "%s",
                    "name": "Monthly Tech Portfolio",
                    "investments": [
                        {"instrument": "AAPL", "amount": 100.00}
                    ],
                    "executionDay": 1
                }
                """.formatted(commandId, userId);

        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll()).hasSize(1)
        );
    }

    @Test
    void shouldNotCreatePlanWhenInvestmentsAreEmpty() throws Exception {
        String commandId = UUID.randomUUID().toString();
        String planName = "Bad Plan " + UUID.randomUUID();
        String message = """
                {
                    "type": "CREATE_PLAN",
                    "commandId": "%s",
                    "userId": "%s",
                    "name": "%s",
                    "investments": [],
                    "executionDay": 1
                }
                """.formatted(commandId, UUID.randomUUID(), planName);

        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planRepository.findAll().stream()
                    .filter(p -> p.getName().equals(planName))
                    .toList()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, commandId);
            assertThat(processed).isEqualTo(1);
        });
    }

    @Test
    void shouldNotCreatePlanWhenExecutionDayIsInvalid() throws Exception {
        String commandId = UUID.randomUUID().toString();
        String planName = "Bad Execution Day " + UUID.randomUUID();
        String message = """
                {
                    "type": "CREATE_PLAN",
                    "commandId": "%s",
                    "userId": "%s",
                    "name": "%s",
                    "investments": [
                        {"instrument": "AAPL", "amount": 100.00}
                    ],
                    "executionDay": 32
                }
                """.formatted(commandId, UUID.randomUUID(), planName);

        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planRepository.findAll().stream()
                    .filter(p -> p.getName().equals(planName))
                    .toList()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, commandId);
            assertThat(processed).isEqualTo(1);
        });
    }

    @Test
    void shouldNotCreatePlanWhenAmountIsNotPositive() throws Exception {
        String commandId = UUID.randomUUID().toString();
        String planName = "Bad Amount " + UUID.randomUUID();
        String message = """
                {
                    "type": "CREATE_PLAN",
                    "commandId": "%s",
                    "userId": "%s",
                    "name": "%s",
                    "investments": [
                        {"instrument": "AAPL", "amount": -1.00}
                    ],
                    "executionDay": 1
                }
                """.formatted(commandId, UUID.randomUUID(), planName);

        producer.send(new ProducerRecord<>("ivp-commands", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planRepository.findAll().stream()
                    .filter(p -> p.getName().equals(planName))
                    .toList()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, commandId);
            assertThat(processed).isEqualTo(1);
        });
    }
}
