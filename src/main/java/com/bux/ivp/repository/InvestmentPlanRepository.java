package com.bux.ivp.repository;

import com.bux.ivp.domain.InvestmentPlan;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.Optional;
import java.util.UUID;

public interface InvestmentPlanRepository extends JpaRepository<InvestmentPlan, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM InvestmentPlan p WHERE p.id = :id")
    Optional<InvestmentPlan> findByIdWithLock(@Param("id") UUID id);
}