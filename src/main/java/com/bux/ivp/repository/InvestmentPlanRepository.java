package com.bux.ivp.repository;

import com.bux.ivp.domain.InvestmentPlan;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface InvestmentPlanRepository extends JpaRepository<InvestmentPlan, UUID> {
}