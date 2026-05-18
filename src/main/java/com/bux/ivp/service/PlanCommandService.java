package com.bux.ivp.service;

import com.bux.ivp.config.KafkaTopics;
import com.bux.ivp.domain.*;
import com.bux.ivp.messaging.consumer.*;
import com.bux.ivp.messaging.producer.*;
import com.bux.ivp.repository.InvestmentPlanRepository;
import com.bux.ivp.repository.PlanExecutionRepository;
import com.bux.ivp.repository.PlanOrderRepository;
import com.bux.ivp.repository.ProcessedMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class PlanCommandService {

    private static final Logger log = LoggerFactory.getLogger(PlanCommandService.class);

    private final InvestmentPlanRepository planRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final PlanExecutionRepository planExecutionRepository;
    private final PlanOrderRepository planOrderRepository;

    public PlanCommandService(InvestmentPlanRepository planRepository,
                              ProcessedMessageRepository processedMessageRepository,
                              KafkaTemplate<String, Object> kafkaTemplate,
                              PlanExecutionRepository planExecutionRepository,
                              PlanOrderRepository planOrderRepository) {
        this.planRepository = planRepository;
        this.processedMessageRepository = processedMessageRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.planExecutionRepository = planExecutionRepository;
        this.planOrderRepository = planOrderRepository;
    }

    @Transactional
    public void handleCreatePlan(CreatePlanCommand cmd) {
        if (cmd.commandId() == null) {
            log.warn("CREATE_PLAN command missing commandId, skipping");
            return;
        }

        int inserted = processedMessageRepository.insertIfAbsent("CREATE_PLAN", cmd.commandId());
        if (inserted == 0) {
            log.info("Duplicate CREATE_PLAN command received, skipping. commandId={}", cmd.commandId());
            return;
        }

        if (!isValid(cmd)) {
            log.warn("Invalid CREATE_PLAN command received. commandId={}", cmd.commandId());
            return;
        }

        InvestmentPlan plan = InvestmentPlan.create(cmd.userId(), cmd.name(), cmd.executionDay());

        cmd.investments().forEach(i ->
                plan.addInvestment(i.instrument(), i.amount())
        );

        planRepository.save(plan);

        List<PlanCreatedEvent.PlanInvestmentDto> investmentDtos = plan.getInvestments().stream()
                .map(i -> new PlanCreatedEvent.PlanInvestmentDto(i.getInstrument(), i.getAmount()))
                .toList();

        PlanCreatedEvent event = new PlanCreatedEvent(
                UUID.randomUUID(),
                plan.getId(),
                plan.getUserId(),
                plan.getName(),
                plan.getExecutionDay(),
                investmentDtos,
                plan.getCreatedAt()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(KafkaTopics.IVP_EVENTS, plan.getId().toString(), event)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.error("Failed to publish PlanCreated event. planId={}", plan.getId(), ex);
                            else log.info("PlanCreated event published. planId={}", plan.getId());
                        });
            }
        });
    }

    @Transactional
    public void handleDeletePlan(DeletePlanCommand cmd) {
        if (cmd.commandId() == null) {
            log.warn("DELETE_PLAN command missing commandId, skipping");
            return;
        }

        int inserted = processedMessageRepository.insertIfAbsent("DELETE_PLAN", cmd.commandId());
        if (inserted == 0) {
            log.info("Duplicate DELETE_PLAN command, skipping. commandId={}", cmd.commandId());
            return;
        }

        if (cmd.planId() == null) {
            log.warn("Invalid DELETE_PLAN command received. commandId={}", cmd.commandId());
            return;
        }

        InvestmentPlan plan = planRepository.findByIdWithLock(cmd.planId()).orElse(null);
        if (plan == null) {
            log.warn("Plan not found for DELETE_PLAN. planId={}", cmd.planId());
            return;
        }

        if (!plan.isActive()) {
            log.warn("Plan already deleted, skipping. planId={}", cmd.planId());
            return;
        }

        plan.delete();
        planRepository.save(plan);

        PlanDeletedEvent event = new PlanDeletedEvent(
                UUID.randomUUID(),
                plan.getId(),
                plan.getUserId(),
                plan.getDeletedAt()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(KafkaTopics.IVP_EVENTS, plan.getId().toString(), event)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.error("Failed to publish PlanDeleted event. planId={}", plan.getId(), ex);
                            else log.info("PlanDeleted event published. planId={}", plan.getId());
                        });
            }
        });
    }

    @Transactional
    public void handleExecutePlan(ExecutePlanCommand cmd) {
        if (cmd.commandId() == null) {
            log.warn("EXECUTE_PLAN command missing commandId, skipping");
            return;
        }

        int inserted = processedMessageRepository.insertIfAbsent("EXECUTE_PLAN", cmd.commandId());
        if (inserted == 0) {
            log.info("Duplicate EXECUTE_PLAN command, skipping. commandId={}", cmd.commandId());
            return;
        }

        if (cmd.planId() == null || cmd.executionDate() == null) {
            log.warn("Invalid EXECUTE_PLAN command received. commandId={}", cmd.commandId());
            return;
        }

        InvestmentPlan plan = planRepository.findByIdWithLock(cmd.planId()).orElse(null);
        if (plan == null) {
            log.warn("Plan not found for EXECUTE_PLAN. planId={}", cmd.planId());
            return;
        }

        if (!plan.isActive()) {
            log.warn("Plan is not active, skipping execution. planId={}", cmd.planId());
            return;
        }

        // Keep the lazy collection loaded before the native insert clears the persistence context.
        List<PlanInvestment> investments = List.copyOf(plan.getInvestments());
        if (investments.isEmpty()) {
            log.warn("Plan has no investments, skipping execution. planId={}", cmd.planId());
            return;
        }

        PlanExecution execution = PlanExecution.create(cmd.planId(), cmd.executionDate());
        int executionInserted = planExecutionRepository.insertIfAbsent(
                execution.getId(), cmd.planId(), cmd.executionDate(), execution.getCreatedAt());
        if (executionInserted == 0) {
            log.info("Plan already executed for date (concurrent insert), skipping. planId={}, executionDate={}",
                    cmd.planId(), cmd.executionDate());
            return;
        }

        List<PlanOrder> orders = investments.stream()
                .map(i -> PlanOrder.create(execution.getId(), plan.getId(), i.getInstrument(), i.getAmount()))
                .toList();

        planOrderRepository.saveAll(orders);

        List<CreateOrderCommand> commands = orders.stream()
                .map(o -> new CreateOrderCommand(o.getOrderId(), plan.getUserId(), plan.getId(), execution.getId(), cmd.executionDate(), o.getInstrument(), o.getAmount()))
                .toList();

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                commands.forEach(c ->
                        kafkaTemplate.send(KafkaTopics.ORDER_COMMANDS, c.orderId().toString(), c)
                                .whenComplete((r, ex) -> {
                                    if (ex != null) log.error("Failed to publish CreateOrder command. orderId={}", c.orderId(), ex);
                                }));
                log.info("CreateOrder commands dispatched. executionId={}, count={}", execution.getId(), commands.size());
            }
        });
    }

    @Transactional
    public void handleOrderExecuted(OrderExecutedEvent event) {
        if (event.eventId() == null || event.orderId() == null) {
            log.warn("ORDER_EXECUTED event missing required fields, skipping");
            return;
        }

        // Do not consume the event id until the correlated order row exists.
        PlanOrder order = planOrderRepository.findByOrderIdWithLock(event.orderId()).orElse(null);
        if (order == null) {
            log.warn("Order not found for ORDER_EXECUTED — not recording as processed to allow retry. orderId={}", event.orderId());
            return;
        }

        int inserted = processedMessageRepository.insertIfAbsent("ORDER_EXECUTED", event.eventId());
        if (inserted == 0) {
            log.info("Duplicate ORDER_EXECUTED event, skipping. eventId={}", event.eventId());
            return;
        }

        if (order.isTerminal()) {
            log.info("Order already terminal, skipping ORDER_EXECUTED event. orderId={}, status={}",
                    order.getOrderId(), order.getStatus());
            return;
        }

        order.markFilled();
        planOrderRepository.saveAndFlush(order);

        PlanOrderFilledEvent filledEvent = new PlanOrderFilledEvent(
                UUID.randomUUID(),
                order.getPlanId(),
                order.getExecutionId(),
                order.getOrderId(),
                order.getInstrument(),
                Instant.now()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(KafkaTopics.IVP_EVENTS, order.getPlanId().toString(), filledEvent)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.error("Failed to publish PlanOrderFilled event. orderId={}", order.getOrderId(), ex);
                            else log.info("PlanOrderFilled event published. orderId={}", order.getOrderId());
                        });
            }
        });

        checkAndCompleteExecution(order.getExecutionId());
    }

    @Transactional
    public void handleOrderFailed(OrderFailedEvent event) {
        if (event.eventId() == null || event.orderId() == null) {
            log.warn("ORDER_FAILED event missing required fields, skipping");
            return;
        }

        // Do not consume the event id until the correlated order row exists.
        PlanOrder order = planOrderRepository.findByOrderIdWithLock(event.orderId()).orElse(null);
        if (order == null) {
            log.warn("Order not found for ORDER_FAILED — not recording as processed to allow retry. orderId={}", event.orderId());
            return;
        }

        int inserted = processedMessageRepository.insertIfAbsent("ORDER_FAILED", event.eventId());
        if (inserted == 0) {
            log.info("Duplicate ORDER_FAILED event, skipping. eventId={}", event.eventId());
            return;
        }

        if (order.isTerminal()) {
            log.info("Order already terminal, skipping ORDER_FAILED event. orderId={}, status={}",
                    order.getOrderId(), order.getStatus());
            return;
        }

        order.markRejected(event.reason());
        planOrderRepository.saveAndFlush(order);

        PlanOrderRejectedEvent rejectedEvent = new PlanOrderRejectedEvent(
                UUID.randomUUID(),
                order.getPlanId(),
                order.getExecutionId(),
                order.getOrderId(),
                order.getInstrument(),
                event.reason(),
                Instant.now()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(KafkaTopics.IVP_EVENTS, order.getPlanId().toString(), rejectedEvent)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.error("Failed to publish PlanOrderRejected event. orderId={}", order.getOrderId(), ex);
                            else log.info("PlanOrderRejected event published. orderId={}", order.getOrderId());
                        });
            }
        });

        checkAndCompleteExecution(order.getExecutionId());
    }

    private void checkAndCompleteExecution(UUID executionId) {
        PlanExecution execution = planExecutionRepository.findByIdWithLock(executionId).orElse(null);
        if (execution == null) return;
        if (execution.getStatus() == ExecutionStatus.COMPLETED) return;

        List<PlanOrder> orders = planOrderRepository.findByExecutionId(executionId);
        boolean allTerminal = orders.stream().allMatch(PlanOrder::isTerminal);
        if (!allTerminal) return;

        long filled = orders.stream().filter(o -> o.getStatus() == OrderStatus.FILLED).count();
        long rejected = orders.stream().filter(o -> o.getStatus() == OrderStatus.REJECTED).count();

        ExecutionResult result;
        if (rejected == 0) result = ExecutionResult.FULLY_FILLED;
        else if (filled == 0) result = ExecutionResult.FULLY_REJECTED;
        else result = ExecutionResult.PARTIALLY_FILLED;

        execution.complete(result);
        planExecutionRepository.save(execution);

        PlanExecutionCompletedEvent completedEvent = new PlanExecutionCompletedEvent(
                UUID.randomUUID(),
                execution.getPlanId(),
                execution.getId(),
                result,
                Instant.now()
        );

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(KafkaTopics.IVP_EVENTS, execution.getPlanId().toString(), completedEvent)
                        .whenComplete((r, ex) -> {
                            if (ex != null) log.error("Failed to publish PlanExecutionCompleted event. executionId={}", execution.getId(), ex);
                            else log.info("PlanExecutionCompleted event published. executionId={}, result={}", execution.getId(), result);
                        });
            }
        });
    }

    private boolean isValid(CreatePlanCommand cmd) {
        if (cmd.userId() == null) return false;
        if (cmd.name() == null || cmd.name().isBlank()) return false;
        if (cmd.executionDay() < 1 || cmd.executionDay() > 31) return false;
        if (cmd.investments() == null || cmd.investments().isEmpty()) return false;
        return cmd.investments().stream()
                .allMatch(i -> i != null
                        && i.instrument() != null
                        && !i.instrument().isBlank()
                        && i.amount() != null
                        && i.amount().signum() > 0);
    }

}
