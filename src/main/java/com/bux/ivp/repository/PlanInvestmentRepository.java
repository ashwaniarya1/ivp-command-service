package com.bux.ivp.repository;

import com.bux.ivp.domain.PlanInvestment;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface PlanInvestmentRepository extends JpaRepository<PlanInvestment, UUID> {
}