package com.bux.ivp.messaging;

import com.bux.ivp.config.KafkaTopics;
import com.bux.ivp.messaging.consumer.*;
import com.bux.ivp.service.PlanCommandService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IvpCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(IvpCommandConsumer.class);

    private final ObjectMapper objectMapper;
    private final PlanCommandService planCommandService;

    public IvpCommandConsumer(ObjectMapper objectMapper, PlanCommandService planCommandService) {
        this.objectMapper = objectMapper;
        this.planCommandService = planCommandService;
    }

    @KafkaListener(topics = KafkaTopics.IVP_COMMANDS, groupId = "${spring.kafka.consumer.group-id:ivp-command-service}")
    public void handle(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            String type = node.path("type").asText();

            switch (type) {
                case "CREATE_PLAN" -> planCommandService.handleCreatePlan(
                        objectMapper.treeToValue(node, CreatePlanCommand.class));
                case "DELETE_PLAN" -> planCommandService.handleDeletePlan(
                        objectMapper.treeToValue(node, DeletePlanCommand.class));
                case "EXECUTE_PLAN" -> planCommandService.handleExecutePlan(
                        objectMapper.treeToValue(node, ExecutePlanCommand.class));
                default -> log.warn("Unknown command type received: {}", type);
            }
        } catch (JsonProcessingException e) {
            log.error("Malformed ivp-commands message, skipping: {}", raw, e);
        } catch (Exception e) {
            log.error("Unexpected failure processing ivp-commands message: {}", raw, e);
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "${spring.kafka.consumer.group-id:ivp-command-service}")
    public void handleOrderEvent(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            String type = node.path("type").asText();

            switch (type) {
                case "ORDER_EXECUTED" -> planCommandService.handleOrderExecuted(
                        objectMapper.treeToValue(node, OrderExecutedEvent.class));
                case "ORDER_FAILED" -> planCommandService.handleOrderFailed(
                        objectMapper.treeToValue(node, OrderFailedEvent.class));
                default -> log.warn("Unknown order event type received: {}", type);
            }
        } catch (JsonProcessingException e) {
            log.error("Malformed order-events message, skipping: {}", raw, e);
        } catch (Exception e) {
            log.error("Unexpected failure processing order-events message: {}", raw, e);
            throw new RuntimeException(e);
        }
    }
}
