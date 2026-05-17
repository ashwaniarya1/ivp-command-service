package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanOrderRejectedEvent(
        UUID eventId,
        UUID planId,
        UUID executionId,
        UUID orderId,
        String instrument,
        String reason,
        Instant occurredAt
) {}
