package com.bux.ivp.messaging.producer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

public record CreateOrderCommand(
        String type,
        UUID commandId,
        UUID orderId,
        UUID userId,
        UUID planId,
        UUID executionId,
        LocalDate executionDate,
        String instrument,
        BigDecimal amount,
        OrderDirection orderDirection
) {
    public CreateOrderCommand(UUID orderId, UUID userId, UUID planId, UUID executionId, LocalDate executionDate, String instrument, BigDecimal amount) {
        this("CREATE_ORDER", UUID.randomUUID(), orderId, userId, planId, executionId, executionDate, instrument, amount, OrderDirection.BUY);
    }
}
