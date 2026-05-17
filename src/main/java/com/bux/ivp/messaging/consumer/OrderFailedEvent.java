package com.bux.ivp.messaging.consumer;

import java.time.Instant;
import java.util.UUID;

public record OrderFailedEvent(
        UUID eventId,
        UUID orderId,
        UUID planId,
        UUID executionId,
        String instrument,
        String reason,
        Instant failedAt
) {}
