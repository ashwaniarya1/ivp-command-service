package com.bux.ivp.repository;

import com.bux.ivp.domain.PlanOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface PlanOrderRepository extends JpaRepository<PlanOrder, UUID> {
}