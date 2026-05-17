package com.bux.ivp.repository;

import com.bux.ivp.domain.InvestmentPlan;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.Optional;
import java.util.UUID;

public interface InvestmentPlanRepository extends JpaRepository<InvestmentPlan, UUID> {

    @Query("SELECT p FROM InvestmentPlan p LEFT JOIN FETCH p.investments WHERE p.id = :id")
    Optional<InvestmentPlan> findByIdWithInvestments(@Param("id") UUID id);
}