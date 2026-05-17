package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanOrderFilledEvent(
        UUID eventId,
        UUID planId,
        UUID executionId,
        UUID orderId,
        String instrument,
        Instant occurredAt
) {}
