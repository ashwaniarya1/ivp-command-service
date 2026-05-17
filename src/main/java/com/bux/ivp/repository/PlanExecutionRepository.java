package com.bux.ivp.repository;

import com.bux.ivp.domain.PlanExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface PlanExecutionRepository extends JpaRepository<PlanExecution, UUID> {
}