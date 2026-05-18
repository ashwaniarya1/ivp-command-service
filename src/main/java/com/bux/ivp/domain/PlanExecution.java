package com.bux.ivp.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

@Entity
@Table(name = "plan_execution",
        uniqueConstraints = @UniqueConstraint(columnNames = {"plan_id", "execution_date"}))
public class PlanExecution {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID planId;

    @Column(nullable = false)
    private LocalDate executionDate;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ExecutionStatus status;

    @Enumerated(EnumType.STRING)
    private ExecutionResult result;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant completedAt;

    protected PlanExecution() {}

    public static PlanExecution create(UUID planId, LocalDate executionDate) {
        var execution = new PlanExecution();
        execution.id = UUID.randomUUID();
        execution.planId = planId;
        execution.executionDate = executionDate;
        execution.status = ExecutionStatus.STARTED;
        execution.createdAt = Instant.now();
        return execution;
    }

    public void complete(ExecutionResult result) {
        this.status = ExecutionStatus.COMPLETED;
        this.result = result;
        this.completedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getPlanId() {
        return planId;
    }

    public LocalDate getExecutionDate() {
        return executionDate;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public ExecutionResult getResult() {
        return result;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}
