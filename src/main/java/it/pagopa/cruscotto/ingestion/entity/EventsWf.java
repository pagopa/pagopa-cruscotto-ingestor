package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "EVENTS_WF")
public class EventsWf {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_events_wf")
    @SequenceGenerator(name = "seq_events_wf", sequenceName = "SQ_EVENTS_WF", allocationSize = 1)
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_POSITION")
    private Integer fkPosition;

    @Column(name = "FK_TOKENS")
    private Integer fkTokens;

    @Column(name = "INSERTED_TIMESTAMP_REQ")
    private LocalDateTime insertedTimestampReq;

    @Column(name = "INSERTED_TIMESTAMP_RESP")
    private LocalDateTime insertedTimestampResp;

    @Column(name = "EVENT_ID_REQ")
    private String eventIdReq;

    @Column(name = "EVENT_ID_RESP")
    private String eventIdResp;

    @Column(name = "FAULT_CODE")
    private Short faultCode;

    @Column(name = "OUTCOME_REQ")
    private String outcomeReq;

    @Column(name = "OUTCOME_RESP")
    private String outcomeResp;

    @Column(name = "TIPO_EVENTO")
    private Short tipoEvento;
}
