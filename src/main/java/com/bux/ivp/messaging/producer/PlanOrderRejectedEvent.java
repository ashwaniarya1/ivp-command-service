package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanOrderRejectedEvent(
        String type,
        UUID eventId,
        UUID planId,
        UUID executionId,
        UUID orderId,
        String instrument,
        String reason,
        Instant occurredAt
) {
    public PlanOrderRejectedEvent(UUID eventId, UUID planId, UUID executionId, UUID orderId, String instrument, String reason, Instant occurredAt) {
        this("PLAN_ORDER_REJECTED", eventId, planId, executionId, orderId, instrument, reason, occurredAt);
    }
}