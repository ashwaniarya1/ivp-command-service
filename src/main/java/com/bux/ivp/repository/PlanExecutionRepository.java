package com.bux.ivp.repository;

import com.bux.ivp.domain.PlanExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

public interface PlanExecutionRepository extends JpaRepository<PlanExecution, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT e FROM PlanExecution e WHERE e.id = :id")
    Optional<PlanExecution> findByIdWithLock(@Param("id") UUID id);

    @Modifying(clearAutomatically = true)
    @Query(value = "INSERT INTO plan_execution (id, plan_id, execution_date, status, created_at) " +
                   "VALUES (:id, :planId, :executionDate, 'STARTED', :createdAt) " +
                   "ON CONFLICT (plan_id, execution_date) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("planId") UUID planId,
                       @Param("executionDate") LocalDate executionDate,
                       @Param("createdAt") Instant createdAt);
}
