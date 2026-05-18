package com.bux.ivp.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "plan_order")
public class PlanOrder {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    @Column(nullable = false)
    private UUID executionId;

    @Column(nullable = false)
    private UUID planId;

    @Column(nullable = false)
    private String instrument;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    private String failureReason;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant completedAt;

    protected PlanOrder() {}

    public static PlanOrder create(UUID executionId, UUID planId, String instrument, BigDecimal amount) {
        var order = new PlanOrder();
        order.id = UUID.randomUUID();
        order.orderId = UUID.randomUUID();
        order.executionId = executionId;
        order.planId = planId;
        order.instrument = instrument;
        order.amount = amount;
        order.status = OrderStatus.PENDING;
        order.createdAt = Instant.now();
        return order;
    }

    public void markFilled() {
        this.status = OrderStatus.FILLED;
        this.completedAt = Instant.now();
    }

    public void markRejected(String reason) {
        this.status = OrderStatus.REJECTED;
        this.failureReason = reason;
        this.completedAt = Instant.now();
    }

    public boolean isTerminal() {
        return this.status == OrderStatus.FILLED || this.status == OrderStatus.REJECTED;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public UUID getPlanId() {
        return planId;
    }

    public String getInstrument() {
        return instrument;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}
