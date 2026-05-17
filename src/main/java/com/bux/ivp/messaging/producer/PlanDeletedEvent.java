package com.bux.ivp.messaging.producer;

import java.time.Instant;
import java.util.UUID;

public record PlanDeletedEvent(
        UUID eventId,
        UUID planId,
        Instant occurredAt
) {}
