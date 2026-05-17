package com.bux.ivp.messaging.consumer;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record CreatePlanCommand(
        UUID commandId,
        UUID userId,
        String name,
        List<PlanInvestmentDto> investments,
        int executionDay
) {
    public record PlanInvestmentDto(
            String instrument,
            BigDecimal amount
    ) {}
}