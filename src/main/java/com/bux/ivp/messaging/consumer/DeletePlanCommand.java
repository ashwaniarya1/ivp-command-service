package com.bux.ivp.messaging;

import java.util.UUID;

public record DeletePlanCommand(
        UUID commandId,
        UUID planId
) {}