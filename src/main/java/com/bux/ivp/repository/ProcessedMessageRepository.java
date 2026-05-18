package com.bux.ivp.repository;

import com.bux.ivp.domain.ProcessedMessage;
import com.bux.ivp.domain.ProcessedMessageId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.UUID;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, ProcessedMessageId> {

    @Modifying(clearAutomatically = true)
    @Query(value = """
            INSERT INTO processed_message(message_type, message_id, processed_at)
            VALUES (:type, :id, now())
            ON CONFLICT (message_type, message_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("type") String type, @Param("id") UUID id);
}