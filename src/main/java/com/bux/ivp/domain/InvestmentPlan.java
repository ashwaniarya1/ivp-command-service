package com.bux.ivp.domain;

import jakarta.persistence.*;
import lombok.Getter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Entity
@Table(name = "investment_plan")
public class InvestmentPlan {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID userId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int executionDay;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PlanStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant deletedAt;

    @OneToMany(mappedBy = "plan", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<PlanInvestment> investments = new ArrayList<>();

    protected InvestmentPlan() {}

    public static InvestmentPlan create(UUID userId, String name, int executionDay) {
        var plan = new InvestmentPlan();
        plan.id = UUID.randomUUID();
        plan.userId = userId;
        plan.name = name;
        plan.executionDay = executionDay;
        plan.status = PlanStatus.ACTIVE;
        plan.createdAt = Instant.now();
        return plan;
    }

    public void delete() {
        this.status = PlanStatus.DELETED;
        this.deletedAt = Instant.now();
    }

    public boolean isActive() {
        return this.status == PlanStatus.ACTIVE;
    }
}