package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Entity
@Table(name = "EVENTS_WF")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EventsWf {

    @Id
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_POSITIONS")
    private Long fkPositions;

    @Column(name = "FK_TOKENS")
    private Long fkTokens;

    @Column(name = "INSERTED_TIMESTAMP")
    private Integer insertedTimestamp;

    @Column(name = "EVENT_ID", length = 30)
    private String eventId;

    @Column(name = "FAULT_CODE")
    private Short faultCode;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "TIPO_EVENTO")
    private Short tipoEvento;

}
