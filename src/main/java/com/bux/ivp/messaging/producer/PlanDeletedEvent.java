package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanDeletedEvent(
        String type,
        UUID eventId,
        UUID planId,
        UUID userId,
        Instant occurredAt
) {
    public PlanDeletedEvent(UUID eventId, UUID planId, UUID userId, Instant occurredAt) {
        this("PLAN_DELETED", eventId, planId, userId, occurredAt);
    }
}