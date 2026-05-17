package com.bux.ivp.messaging.producer;

import com.bux.ivp.domain.ExecutionResult;
import java.time.Instant;
import java.util.UUID;

public record PlanExecutionCompletedEvent(
        UUID eventId,
        UUID planId,
        UUID executionId,
        ExecutionResult result,
        Instant occurredAt
) {}
