package com.bux.ivp.messaging;

import com.bux.ivp.messaging.consumer.CreatePlanCommand;
import com.bux.ivp.service.PlanCommandService;
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

    @KafkaListener(topics = "ivp-commands", groupId = "ivp-command-service")
    public void handle(String raw) {
        try {
            JsonNode node = objectMapper.readTree(raw);
            String type = node.get("type").asText();

            switch (type) {
                case "CREATE_PLAN" -> planCommandService.handleCreatePlan(
                        objectMapper.treeToValue(node, CreatePlanCommand.class));
                default -> log.warn("Unknown command type received: {}", type);
            }
        } catch (Exception e) {
            log.error("Failed to process command: {}", raw, e);
        }
    }
}