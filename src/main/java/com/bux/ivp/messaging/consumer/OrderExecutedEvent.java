package com.bux.ivp.messaging.consumer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderExecutedEvent(
        UUID eventId,
        UUID orderId,
        UUID planId,
        UUID executionId,
        String instrument,
        BigDecimal filledAmount,
        Instant executedAt
) {}
