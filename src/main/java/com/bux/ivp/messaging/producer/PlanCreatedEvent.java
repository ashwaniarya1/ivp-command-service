package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanCreatedEvent(
        String type,
        UUID eventId,
        UUID planId,
        UUID userId,
        String name,
        int executionDay,
        Instant occurredAt
) {
    public PlanCreatedEvent(UUID eventId, UUID planId, UUID userId, String name, int executionDay, Instant occurredAt) {
        this("PLAN_CREATED", eventId, planId, userId, name, executionDay, occurredAt);
    }
}