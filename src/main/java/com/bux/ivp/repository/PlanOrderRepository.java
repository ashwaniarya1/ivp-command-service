package com.bux.ivp.repository;

import com.bux.ivp.domain.PlanOrder;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PlanOrderRepository extends JpaRepository<PlanOrder, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM PlanOrder o WHERE o.orderId = :orderId")
    Optional<PlanOrder> findByOrderIdWithLock(@Param("orderId") UUID orderId);

    List<PlanOrder> findByExecutionId(UUID executionId);
}