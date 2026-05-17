package com.bux.ivp.messaging.consumer;

import java.util.UUID;

public record DeletePlanCommand(
        UUID commandId,
        UUID planId
) {}