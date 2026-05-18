package com.bux.ivp;

import com.bux.ivp.domain.ExecutionResult;
import com.bux.ivp.domain.ExecutionStatus;
import com.bux.ivp.domain.OrderStatus;
import com.bux.ivp.messaging.consumer.OrderExecutedEvent;
import com.bux.ivp.messaging.consumer.OrderFailedEvent;
import com.bux.ivp.repository.InvestmentPlanRepository;
import com.bux.ivp.repository.PlanExecutionRepository;
import com.bux.ivp.repository.PlanOrderRepository;
import com.bux.ivp.service.PlanCommandService;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
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
class OrderResultIntegrationTest {

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
    private PlanCommandService planCommandService;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> ivpEventConsumer;

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

        ivpEventConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-ivp-event-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        ivpEventConsumer.subscribe(List.of("ivp-events"));
    }

    @AfterEach
    void tearDown() {
        if (ivpEventConsumer != null) ivpEventConsumer.close();
        if (producer != null) producer.close();
    }

    private String[] setupPlanAndExecution(String planName) throws Exception {
        String userId = UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>("ivp-commands", """
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
                """.formatted(UUID.randomUUID(), userId, planName)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planRepository.findAll().stream()
                        .filter(p -> p.getName().equals(planName)).toList()).hasSize(1)
        );

        String planId = planRepository.findAll().stream()
                .filter(p -> p.getName().equals(planName))
                .findFirst().get().getId().toString();

        producer.send(new ProducerRecord<>("ivp-commands", """
                {
                    "type": "EXECUTE_PLAN",
                    "commandId": "%s",
                    "planId": "%s",
                    "executionDate": "2026-05-17"
                }
                """.formatted(UUID.randomUUID(), planId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(planOrderRepository.findAll()).hasSize(2)
        );

        String executionId = planExecutionRepository.findAll().get(0).getId().toString();
        String aaplOrderId = planOrderRepository.findAll().stream()
                .filter(o -> o.getInstrument().equals("AAPL"))
                .findFirst().get().getOrderId().toString();
        String googlOrderId = planOrderRepository.findAll().stream()
                .filter(o -> o.getInstrument().equals("GOOGL"))
                .findFirst().get().getOrderId().toString();

        return new String[]{planId, executionId, aaplOrderId, googlOrderId};
    }

    private List<JsonNode> collectEventsForExecution(String executionId, int expectedCount) {
        List<JsonNode> matchingEvents = new ArrayList<>();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var records = ivpEventConsumer.poll(Duration.ofSeconds(1));
            for (var record : records) {
                JsonNode node = objectMapper.readTree(record.value());
                JsonNode executionIdNode = node.get("executionId");
                if (executionIdNode != null && executionId.equals(executionIdNode.asText())) {
                    matchingEvents.add(node);
                }
            }
            assertThat(matchingEvents).hasSize(expectedCount);
        });

        return matchingEvents;
    }

    private List<JsonNode> collectAtLeastEventsForExecution(String executionId, int expectedCount) throws Exception {
        List<JsonNode> matchingEvents = new ArrayList<>();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            collectAvailableEvents(executionId, matchingEvents, Duration.ofSeconds(1));
            assertThat(matchingEvents.size()).isGreaterThanOrEqualTo(expectedCount);
        });

        collectAvailableEvents(executionId, matchingEvents, Duration.ofSeconds(2));
        return matchingEvents;
    }

    private void collectAvailableEvents(String executionId, List<JsonNode> matchingEvents, Duration timeout) throws Exception {
        var records = ivpEventConsumer.poll(timeout);
        for (var record : records) {
            JsonNode node = objectMapper.readTree(record.value());
            JsonNode executionIdNode = node.get("executionId");
            if (executionIdNode != null && executionId.equals(executionIdNode.asText())) {
                matchingEvents.add(node);
            }
        }
    }

    @Test
    void shouldCompleteExecutionAsFullyFilledWhenAllOrdersFilled() throws Exception {
        String planName = "Fully Filled Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        String planId = ids[0];
        String executionId = ids[1];
        String aaplOrderId = ids[2];
        String googlOrderId = ids[3];

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "filledAmount": 100.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), aaplOrderId, planId, executionId)));

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "GOOGL",
                    "filledAmount": 50.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), googlOrderId, planId, executionId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = planExecutionRepository.findById(UUID.fromString(executionId));
            assertThat(execution).isPresent();
            assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(execution.get().getResult()).isEqualTo(ExecutionResult.FULLY_FILLED);
        });

        var orders = planOrderRepository.findAll();
        assertThat(orders).allMatch(o -> o.getStatus() == OrderStatus.FILLED);

        List<JsonNode> events = collectEventsForExecution(executionId, 3);
        collectAvailableEvents(executionId, events, Duration.ofSeconds(2));
        assertThat(events.stream().map(e -> e.get("type").asText()).toList())
                .containsExactly("PLAN_ORDER_FILLED", "PLAN_ORDER_FILLED", "PLAN_EXECUTION_COMPLETED");
        assertThat(events.get(2).get("result").asText()).isEqualTo("FULLY_FILLED");
    }

    @Test
    void shouldCompleteExecutionAsFullyRejectedWhenAllOrdersFailed() throws Exception {
        String planName = "Fully Rejected Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        String planId = ids[0];
        String executionId = ids[1];
        String aaplOrderId = ids[2];
        String googlOrderId = ids[3];

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_FAILED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "reason": "Insufficient funds",
                    "failedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), aaplOrderId, planId, executionId)));

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_FAILED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "GOOGL",
                    "reason": "Market closed",
                    "failedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), googlOrderId, planId, executionId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = planExecutionRepository.findById(UUID.fromString(executionId));
            assertThat(execution).isPresent();
            assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(execution.get().getResult()).isEqualTo(ExecutionResult.FULLY_REJECTED);
        });

        var orders = planOrderRepository.findAll();
        assertThat(orders).allMatch(o -> o.getStatus() == OrderStatus.REJECTED);

        List<JsonNode> events = collectEventsForExecution(executionId, 3);
        collectAvailableEvents(executionId, events, Duration.ofSeconds(2));
        assertThat(events.stream().map(e -> e.get("type").asText()).toList())
                .containsExactly("PLAN_ORDER_REJECTED", "PLAN_ORDER_REJECTED", "PLAN_EXECUTION_COMPLETED");
        assertThat(events.get(2).get("result").asText()).isEqualTo("FULLY_REJECTED");
    }

    @Test
    void shouldCompleteExecutionAsPartiallyFilledWhenMixedResults() throws Exception {
        String planName = "Partially Filled Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        String planId = ids[0];
        String executionId = ids[1];
        String aaplOrderId = ids[2];
        String googlOrderId = ids[3];

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "filledAmount": 100.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), aaplOrderId, planId, executionId)));

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_FAILED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "GOOGL",
                    "reason": "Insufficient funds",
                    "failedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), googlOrderId, planId, executionId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = planExecutionRepository.findById(UUID.fromString(executionId));
            assertThat(execution).isPresent();
            assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(execution.get().getResult()).isEqualTo(ExecutionResult.PARTIALLY_FILLED);
        });

        var aapl = planOrderRepository.findAll().stream()
                .filter(o -> o.getInstrument().equals("AAPL")).findFirst().get();
        var googl = planOrderRepository.findAll().stream()
                .filter(o -> o.getInstrument().equals("GOOGL")).findFirst().get();

        assertThat(aapl.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(googl.getStatus()).isEqualTo(OrderStatus.REJECTED);

        List<JsonNode> events = collectEventsForExecution(executionId, 3);
        collectAvailableEvents(executionId, events, Duration.ofSeconds(2));
        assertThat(events.stream().map(e -> e.get("type").asText()).toList())
                .containsExactly("PLAN_ORDER_FILLED", "PLAN_ORDER_REJECTED", "PLAN_EXECUTION_COMPLETED");
        assertThat(events.get(2).get("result").asText()).isEqualTo("PARTIALLY_FILLED");
    }

    @Test
    void shouldEmitPlanExecutionCompletedOnceWhenLastOrderResultsArriveConcurrently() throws Exception {
        String planName = "Concurrent Completion Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        UUID planId = UUID.fromString(ids[0]);
        String executionId = ids[1];
        UUID executionUuid = UUID.fromString(executionId);
        UUID aaplOrderId = UUID.fromString(ids[2]);
        UUID googlOrderId = UUID.fromString(ids[3]);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);

        Future<?> filled = executor.submit(() -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            planCommandService.handleOrderExecuted(new OrderExecutedEvent(
                    UUID.randomUUID(),
                    aaplOrderId,
                    planId,
                    executionUuid,
                    "AAPL",
                    new BigDecimal("100.00"),
                    Instant.parse("2026-05-17T20:00:00Z")
            ));
            return null;
        });

        Future<?> rejected = executor.submit(() -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            planCommandService.handleOrderFailed(new OrderFailedEvent(
                    UUID.randomUUID(),
                    googlOrderId,
                    planId,
                    executionUuid,
                    "GOOGL",
                    "Market closed",
                    Instant.parse("2026-05-17T20:00:00Z")
            ));
            return null;
        });

        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        filled.get(10, TimeUnit.SECONDS);
        rejected.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var execution = planExecutionRepository.findById(executionUuid);
            assertThat(execution).isPresent();
            assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(execution.get().getResult()).isEqualTo(ExecutionResult.PARTIALLY_FILLED);
        });

        List<JsonNode> events = collectAtLeastEventsForExecution(executionId, 3);
        assertThat(events.stream().map(e -> e.get("type").asText()).toList())
                .containsExactlyInAnyOrder("PLAN_ORDER_FILLED", "PLAN_ORDER_REJECTED", "PLAN_EXECUTION_COMPLETED");
        assertThat(events.stream()
                .filter(e -> e.get("type").asText().equals("PLAN_EXECUTION_COMPLETED")))
                .hasSize(1);
    }

    @Test
    void shouldIgnoreDuplicateOrderResult() throws Exception {
        String planName = "Duplicate Order Result Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        String planId = ids[0];
        String executionId = ids[1];
        String aaplOrderId = ids[2];

        String eventId = UUID.randomUUID().toString();
        String message = """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "filledAmount": 100.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(eventId, aaplOrderId, planId, executionId);

        producer.send(new ProducerRecord<>("order-events", message));
        producer.send(new ProducerRecord<>("order-events", message));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var aapl = planOrderRepository.findAll().stream()
                    .filter(o -> o.getInstrument().equals("AAPL")).findFirst().get();
            assertThat(aapl.getStatus()).isEqualTo(OrderStatus.FILLED);
        });

        var execution = planExecutionRepository.findById(UUID.fromString(executionId));
        assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.STARTED);

        List<JsonNode> events = collectEventsForExecution(executionId, 1);
        assertThat(events.get(0).get("type").asText()).isEqualTo("PLAN_ORDER_FILLED");
    }

    @Test
    void shouldNotOverwriteTerminalOrderResult() throws Exception {
        String planName = "Terminal Order Result Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        String planId = ids[0];
        String executionId = ids[1];
        String aaplOrderId = ids[2];

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "filledAmount": 100.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(UUID.randomUUID(), aaplOrderId, planId, executionId)));
        producer.flush();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var aapl = planOrderRepository.findAll().stream()
                    .filter(o -> o.getInstrument().equals("AAPL")).findFirst().get();
            assertThat(aapl.getStatus()).isEqualTo(OrderStatus.FILLED);
        });

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_FAILED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "reason": "Late rejection",
                    "failedAt": "2026-05-17T20:01:00Z"
                }
                """.formatted(UUID.randomUUID(), aaplOrderId, planId, executionId)));
        producer.flush();

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var aapl = planOrderRepository.findAll().stream()
                    .filter(o -> o.getInstrument().equals("AAPL")).findFirst().get();
            assertThat(aapl.getStatus()).isEqualTo(OrderStatus.FILLED);
            assertThat(aapl.getFailureReason()).isNull();

            var execution = planExecutionRepository.findById(UUID.fromString(executionId));
            assertThat(execution.get().getStatus()).isEqualTo(ExecutionStatus.STARTED);
        });

        List<JsonNode> events = collectEventsForExecution(executionId, 1);
        assertThat(events.get(0).get("type").asText()).isEqualTo("PLAN_ORDER_FILLED");
    }

    @Test
    void shouldNotOverwriteTerminalOrderResultWhenConflictingResultsArriveConcurrently() throws Exception {
        String planName = "Concurrent Same Order Test " + UUID.randomUUID();
        String[] ids = setupPlanAndExecution(planName);
        UUID planId = UUID.fromString(ids[0]);
        String executionId = ids[1];
        UUID executionUuid = UUID.fromString(executionId);
        UUID aaplOrderId = UUID.fromString(ids[2]);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);

        Future<?> filled = executor.submit(() -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            planCommandService.handleOrderExecuted(new OrderExecutedEvent(
                    UUID.randomUUID(),
                    aaplOrderId,
                    planId,
                    executionUuid,
                    "AAPL",
                    new BigDecimal("100.00"),
                    Instant.parse("2026-05-17T20:00:00Z")
            ));
            return null;
        });

        Future<?> failed = executor.submit(() -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            planCommandService.handleOrderFailed(new OrderFailedEvent(
                    UUID.randomUUID(),
                    aaplOrderId,
                    planId,
                    executionUuid,
                    "AAPL",
                    "Conflicting rejection",
                    Instant.parse("2026-05-17T20:00:00Z")
            ));
            return null;
        });

        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        filled.get(10, TimeUnit.SECONDS);
        failed.get(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        var aapl = planOrderRepository.findAll().stream()
                .filter(o -> o.getInstrument().equals("AAPL")).findFirst().get();
        assertThat(aapl.getStatus()).isIn(OrderStatus.FILLED, OrderStatus.REJECTED);

        List<JsonNode> events = collectAtLeastEventsForExecution(executionId, 1);
        collectAvailableEvents(executionId, events, Duration.ofSeconds(3));
        long aaplResultEvents = events.stream()
                .filter(e -> {
                    String type = e.get("type").asText();
                    return type.equals("PLAN_ORDER_FILLED") || type.equals("PLAN_ORDER_REJECTED");
                })
                .filter(e -> {
                    JsonNode inst = e.get("instrument");
                    return inst != null && "AAPL".equals(inst.asText());
                })
                .count();
        assertThat(aaplResultEvents).isEqualTo(1);
    }

    @Test
    void shouldIgnoreOrderResultForUnknownOrderId() throws Exception {
        String eventId = UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>("order-events", """
                {
                    "type": "ORDER_EXECUTED",
                    "eventId": "%s",
                    "orderId": "%s",
                    "planId": "%s",
                    "executionId": "%s",
                    "instrument": "AAPL",
                    "filledAmount": 100.00,
                    "executedAt": "2026-05-17T20:00:00Z"
                }
                """.formatted(eventId, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())));
        producer.flush();

        // Unknown orders must leave the event id available for a later retry.
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(planOrderRepository.findAll()).isEmpty();
            assertThat(planExecutionRepository.findAll()).isEmpty();
            Integer processed = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM processed_message WHERE message_id = ?::uuid",
                    Integer.class, eventId);
            assertThat(processed).isEqualTo(0);
        });
    }
}
