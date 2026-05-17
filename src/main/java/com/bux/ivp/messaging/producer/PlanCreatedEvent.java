package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanCreatedEvent(
        UUID eventId,
        UUID planId,
        UUID userId,
        String name,
        int executionDay,
        Instant occurredAt
) {}
