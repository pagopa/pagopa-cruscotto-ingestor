package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "STG_INGEST_ERROR")
public class StagingIngestError {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;

    @Column(name = "RUN_ID")
    private String runId;

    @Column(name = "ENTITY_NAME")
    private String entityName;

    @Column(name = "SOURCE_KEY")
    private String sourceKey;

    @Column(name = "PAYLOAD_JSON", columnDefinition = "jsonb")
    private String payloadJson;

    @Column(name = "ERROR_CODE")
    private String errorCode;

    @Column(name = "ERROR_MESSAGE", columnDefinition = "text")
    private String errorMessage;

    @Column(name = "CREATED_AT")
    private OffsetDateTime createdAt;

    @Column(name = "STATUS")
    @Enumerated(EnumType.STRING)
    private StagingStatus status;

    @Column(name = "RETRY_COUNT")
    private Integer retryCount;

    @Column(name = "LAST_RETRY_AT")
    private OffsetDateTime lastRetryAt;
}

