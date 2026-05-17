package com.bux.ivp.messaging;

import java.time.LocalDate;
import java.util.UUID;

public record ExecutePlanCommand(
        UUID commandId,
        UUID planId,
        LocalDate executionDate
) {}