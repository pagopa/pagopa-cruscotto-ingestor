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
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "POSITIONS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Positions {

    @Id
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "INSERTED_TIMESTAMP")
    private LocalDateTime insertedTimestamp;

    @Column(name = "NAV", length = 18)
    private String nav;

    @Column(name = "PA_EMITTENTE", length = 11)
    private String paEmittente;

    @Column(name = "LAST_EVENT")
    private LocalDateTime lastEvent;

    @Column(name = "DATE_EVENTS", columnDefinition = "json")
    private String dateEvents;

    @Column(name = "UUID_POSITION")
    private UUID uuidPosition;

}
