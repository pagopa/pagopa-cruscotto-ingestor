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
    @Column(name = "UNIQUE_ID")
    private String uniqueId;

    @Column(name = "INSERTED_TIMESTAMP")
    private LocalDateTime insertedTimestamp;

    @Column(name = "NAV", length = 50)
    private String nav;

    @Column(name = "PA_EMITTENTE", length = 50)
    private String paEmittente;

    @Column(name = "IUV", length = 35)
    private String iuv;

    @Column(name = "TOKEN")
    private String token;

    @Column(name = "STAZIONE")
    private String stazione;

    @Column(name = "INTERMEDIARIO_PA")
    private String intermediarioPa;

    @Column(name = "PSP")
    private String psp;

    @Column(name = "CANALE")
    private String canale;

    @Column(name = "INTERMEDIARIO_PSP")
    private String intermediarioPsp;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "FAULT_CODE")
    private String faultCode;

    @Column(name = "SESSION_ID")
    private String sessionId;

    @Column(name = "TIPO_EVENTO")
    private String tipoEvento;

    @Column(name = "SOTTO_TIPO_EVENTO")
    private String sottoTipoEvento;

    @Column(name = "SERVICE_IDENTIFIER")
    private String serviceIdentifier;

}
