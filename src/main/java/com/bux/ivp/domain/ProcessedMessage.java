package com.bux.ivp.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_message")
@IdClass(ProcessedMessageId.class)
public class ProcessedMessage {

    @Id
    private String messageType;

    @Id
    private UUID messageId;

    @Column(nullable = false)
    private Instant processedAt;

    protected ProcessedMessage() {}
}