package com.bux.ivp.service;

import com.bux.ivp.domain.InvestmentPlan;
import com.bux.ivp.domain.PlanInvestment;
import com.bux.ivp.messaging.consumer.CreatePlanCommand;
import com.bux.ivp.messaging.producer.PlanCreatedEvent;
import com.bux.ivp.repository.InvestmentPlanRepository;
import com.bux.ivp.repository.ProcessedMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

@Service
public class PlanCommandService {

    private static final Logger log = LoggerFactory.getLogger(PlanCommandService.class);

    private final InvestmentPlanRepository planRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PlanCommandService(InvestmentPlanRepository planRepository,
                              ProcessedMessageRepository processedMessageRepository,
                              KafkaTemplate<String, Object> kafkaTemplate) {
        this.planRepository = planRepository;
        this.processedMessageRepository = processedMessageRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public void handleCreatePlan(CreatePlanCommand cmd) {
        // idempotency check - if already processed, skip
        int inserted = processedMessageRepository.insertIfAbsent("CREATE_PLAN", cmd.commandId());
        if (inserted == 0) {
            log.info("Duplicate CREATE_PLAN command received, skipping. commandId={}", cmd.commandId());
            return;
        }

        // validate
        if (!isValid(cmd)) {
            log.warn("Invalid CREATE_PLAN command received. commandId={}", cmd.commandId());
            return;
        }

        // create plan
        InvestmentPlan plan = InvestmentPlan.create(cmd.userId(), cmd.name(), cmd.executionDay());

        // add investments
        cmd.investments().forEach(i ->
                plan.getInvestments().add(PlanInvestment.create(plan, i.instrument(), i.amount()))
        );

        planRepository.save(plan);

        // publish event after commit
        PlanCreatedEvent event = new PlanCreatedEvent(
                UUID.randomUUID(),
                plan.getId(),
                plan.getUserId(),
                plan.getName(),
                plan.getExecutionDay(),
                plan.getCreatedAt()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send("ivp-events", plan.getId().toString(), event);
                log.info("PlanCreated event published. planId={}", plan.getId());
            }
        });
    }

    private boolean isValid(CreatePlanCommand cmd) {
        if (cmd.userId() == null) return false;
        if (cmd.name() == null || cmd.name().isBlank()) return false;
        if (cmd.executionDay() < 1 || cmd.executionDay() > 31) return false;
        if (cmd.investments() == null || cmd.investments().isEmpty()) return false;
        return cmd.investments().stream()
                .allMatch(i -> i.instrument() != null
                        && !i.instrument().isBlank()
                        && i.amount() != null
                        && i.amount().signum() > 0);
    }
}