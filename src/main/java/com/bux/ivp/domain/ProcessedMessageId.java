package com.bux.ivp.domain;

import java.io.Serializable;
import java.util.UUID;
import java.util.Objects;

public class ProcessedMessageId implements Serializable {

    private String messageType;
    private UUID messageId;

    public ProcessedMessageId() {}

    public ProcessedMessageId(String messageType, UUID messageId) {
        this.messageType = messageType;
        this.messageId = messageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProcessedMessageId that)) return false;
        return Objects.equals(messageType, that.messageType) &&
                Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageType, messageId);
    }
}