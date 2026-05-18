package com.bux.ivp.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "plan_investment")
public class PlanInvestment {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "plan_id", nullable = false)
    private InvestmentPlan plan;

    @Column(nullable = false)
    private String instrument;

    @Column(nullable = false)
    private BigDecimal amount;

    protected PlanInvestment() {}

    public static PlanInvestment create(InvestmentPlan plan, String instrument, BigDecimal amount) {
        var investment = new PlanInvestment();
        investment.id = UUID.randomUUID();
        investment.plan = plan;
        investment.instrument = instrument;
        investment.amount = amount;
        return investment;
    }

    public UUID getId() {
        return id;
    }

    public InvestmentPlan getPlan() {
        return plan;
    }

    public String getInstrument() {
        return instrument;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
