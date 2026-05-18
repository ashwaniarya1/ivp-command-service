package com.bux.ivp;

import com.bux.ivp.domain.ExecutionStatus;
import com.bux.ivp.domain.OrderStatus;
import com.bux.ivp.domain.PlanExecution;
import com.bux.ivp.repository.InvestmentPlanRepository;
import com.bux.ivp.repository.PlanExecutionRepository;
import com.bux.ivp.repository.PlanOrderRepository;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExecutePlanIntegrationTest {

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
    private PlanExecutionRepository planExecutionRepository;

    @Autowired
    private PlanOrderRepository planOrderRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> orderCommandConsumer;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("DELETE FROM plan_order");
        jdbcTemplate.execute("DELETE FROM plan_execution");
        jdbcTemplate.execute("DELETE FROM plan_investment");
        jdbcTemplate.execute("DELETE FROM investment_plan");
        jdbcTemplate.execute("DELETE FROM processed_message");

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        orderCommandConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-order-command-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        orderCommandConsumer.subscribe(List.of("order-commands"));
    }

    @AfterEach
    void tearDown() {
        if (orderCommandConsumer != null) orderCommandConsumer.close();
        if (producer != null) producer.close();
    }

    private String createPlan(String commandId, String userId, String name) {
        return """
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
                """.formatted(commandId, userId, name);
    }

    private String executePlan(String commandId, String planId, String date) {
        return """
                {
                    "type": "EXECUTE_PLAN",
                    "commandId": "%s",
                    "planId": "%s",
                    "executionDate": "%s"
                }
                """.formatted(commandId, planId, date);
    }

    @Test
    void shouldExecutePlanAndCreateOrders() throws Exception {
        String userId = UUID.randomUUID().toString();
        String planName = "Execution Test " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(UUID.randomUUID().toString(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        producer.send(new ProducerRecord<>("ivp-commands", executePlan(UUID.randomUUID().toString(), planId, "2026-05-17")));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var executions = planExecutionRepository.findAll();
            assertThat(executions).hasSize(1);
            assertThat(executions.get(0).getStatus()).isEqualTo(ExecutionStatus.STARTED);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var orders = planOrderRepository.findAll();
            assertThat(orders).hasSize(2);
            assertThat(orders).allMatch(o -> o.getStatus() == OrderStatus.PENDING);
        });

        List<JsonNode> matchingOrderCommands = new ArrayList<>();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var records = orderCommandConsumer.poll(Duration.ofSeconds(1));
            for (var record : records) {
                var node = objectMapper.readTree(record.value());
                if (planId.equals(node.get("planId").asText())) {
                    matchingOrderCommands.add(node);
                    assertThat(record.key()).isEqualTo(node.get("orderId").asText());
                }
            }

            assertThat(matchingOrderCommands).hasSize(2);
            assertThat(matchingOrderCommands).allSatisfy(node -> {
                assertThat(node.get("type").asText()).isEqualTo("CREATE_ORDER");
                assertThat(node.get("commandId").asText()).isNotBlank();
                assertThat(node.get("userId").asText()).isEqualTo(userId);
                assertThat(node.get("planId").asText()).isEqualTo(planId);
                assertThat(node.get("orderDirection").asText()).isEqualTo("BUY");
                assertThat(node.get("orderId").asText()).isNotBlank();
                assertThat(node.get("executionId").asText()).isNotBlank();
                assertThat(node.get("executionDate").asText()).isEqualTo("2026-05-17");
                assertThat(node.get("instrument").asText()).isIn("AAPL", "GOOGL");
                assertThat(node.get("amount").decimalValue()).isPositive();
            });
        });
    }

    @Test
    void shouldIgnoreDuplicateExecuteCommand() throws Exception {
        String userId = UUID.randomUUID().toString();
        String planName = "Duplicate Execute Test " + UUID.randomUUID();
        String executeCommandId = UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(UUID.randomUUID().toString(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        producer.send(new ProducerRecord<>("ivp-commands", executePlan(executeCommandId, planId, "2026-05-17")));
        producer.send(new ProducerRecord<>("ivp-commands", executePlan(executeCommandId, planId, "2026-05-17")));
        producer.flush();

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planExecutionRepository.findAll()).hasSize(1);
            assertThat(planOrderRepository.findAll()).hasSize(2);
        });
    }

    @Test
    void shouldIgnoreDuplicateExecutionForSamePlanAndDateWithDifferentCommandId() throws Exception {
        String userId = UUID.randomUUID().toString();
        String planName = "Same Date Duplicate Execute Test " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(UUID.randomUUID().toString(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        producer.send(new ProducerRecord<>("ivp-commands", executePlan(UUID.randomUUID().toString(), planId, "2026-05-17")));
        producer.send(new ProducerRecord<>("ivp-commands", executePlan(UUID.randomUUID().toString(), planId, "2026-05-17")));
        producer.flush();

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
            assertThat(planExecutionRepository.findAll()).hasSize(1);
            assertThat(planOrderRepository.findAll()).hasSize(2);
        });
    }

    @Test
    void shouldNotExecuteDeletedPlan() throws Exception {
        String userId = UUID.randomUUID().toString();
        String planName = "Deleted Plan Execute Test " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(UUID.randomUUID().toString(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        producer.send(new ProducerRecord<>("ivp-commands",
                """
                {"type":"DELETE_PLAN","commandId":"%s","planId":"%s"}
                """.formatted(UUID.randomUUID(), planId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var plan = planRepository.findById(UUID.fromString(planId));
            assertThat(plan.get().getStatus().name()).isEqualTo("DELETED");
        });

        producer.send(new ProducerRecord<>("ivp-commands", executePlan(UUID.randomUUID().toString(), planId, "2026-05-17")));
        producer.flush();

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planExecutionRepository.findAll()).isEmpty();
            assertThat(planOrderRepository.findAll()).isEmpty();
        });
    }

    @Test
    void shouldIgnoreExecutePlanForUnknownPlanId() throws Exception {
        String commandId = UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>("ivp-commands",
                executePlan(commandId, UUID.randomUUID().toString(), "2026-05-17")));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planExecutionRepository.findAll()).isEmpty();
            assertThat(planOrderRepository.findAll()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, commandId);
            assertThat(processed).isEqualTo(1);
        });
    }

    @Test
    void shouldIgnoreExecutePlanWhenExecutionDateIsMissing() throws Exception {
        String userId = UUID.randomUUID().toString();
        String planName = "Missing Execution Date Test " + UUID.randomUUID();

        producer.send(new ProducerRecord<>("ivp-commands", createPlan(UUID.randomUUID().toString(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        String commandId = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>("ivp-commands", """
                {
                    "type": "EXECUTE_PLAN",
                    "commandId": "%s",
                    "planId": "%s",
                    "executionDate": null
                }
                """.formatted(commandId, planId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, commandId);
            assertThat(processed).isEqualTo(1);
            assertThat(planExecutionRepository.findAll()).isEmpty();
            assertThat(planOrderRepository.findAll()).isEmpty();
        });
    }

    @Test
    void shouldDropExecutePlanArrivingBeforeCreatePlan() throws Exception {
        UUID futurePlanId = UUID.randomUUID();
        String executeCommandId = UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>("ivp-commands",
                executePlan(executeCommandId, futurePlanId.toString(), "2026-05-17")));
        producer.flush();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planExecutionRepository.findAll()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, executeCommandId);
            assertThat(processed).isEqualTo(1);
        });
    }

    @Test
    void shouldInsertExactlyOneExecutionWhenConcurrentInsertForSamePlanAndDate() throws Exception {
        // Exercise the database uniqueness guard directly: two transactions try to
        // create the same plan/date execution, and PostgreSQL should accept only one.
        UUID planId = UUID.randomUUID();
        LocalDate executionDate = LocalDate.of(2026, 5, 17);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    start.await(5, TimeUnit.SECONDS);
                    return txTemplate.execute(status -> {
                        PlanExecution execution = PlanExecution.create(planId, executionDate);
                        return planExecutionRepository.insertIfAbsent(
                                execution.getId(), planId, executionDate, execution.getCreatedAt());
                    });
                }));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<Integer> insertResults = new ArrayList<>();
            for (Future<Integer> f : futures) {
                insertResults.add(f.get(10, TimeUnit.SECONDS));
            }

            assertThat(insertResults).containsExactlyInAnyOrder(1, 0);
            assertThat(planExecutionRepository.findAll())
                    .singleElement()
                    .satisfies(execution -> {
                        assertThat(execution.getPlanId()).isEqualTo(planId);
                        assertThat(execution.getExecutionDate()).isEqualTo(executionDate);
                    });
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
