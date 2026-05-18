package com.bux.ivp.messaging.producer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record PlanCreatedEvent(
        String type,
        UUID eventId,
        UUID planId,
        UUID userId,
        String name,
        int executionDay,
        List<PlanInvestmentDto> investments,
        Instant occurredAt
) {
    public record PlanInvestmentDto(String instrument, BigDecimal amount) {}

    public PlanCreatedEvent(UUID eventId, UUID planId, UUID userId, String name, int executionDay, List<PlanInvestmentDto> investments, Instant occurredAt) {
        this("PLAN_CREATED", eventId, planId, userId, name, executionDay, investments, occurredAt);
    }
}