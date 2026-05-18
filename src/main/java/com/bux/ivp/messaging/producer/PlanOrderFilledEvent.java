package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanOrderFilledEvent(
        String type,
        UUID eventId,
        UUID planId,
        UUID executionId,
        UUID orderId,
        String instrument,
        Instant occurredAt
) {
    public PlanOrderFilledEvent(UUID eventId, UUID planId, UUID executionId, UUID orderId, String instrument, Instant occurredAt) {
        this("PLAN_ORDER_FILLED", eventId, planId, executionId, orderId, instrument, occurredAt);
    }
}