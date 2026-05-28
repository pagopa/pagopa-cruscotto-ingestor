package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "DATALAYER_POSITION_TRANSFERS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataLayerPositionTransfers {

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

    @Column(name = "ID_TRANSFER")
    private Integer idTransfer;

    @Column(name = "TRANSFER_AMOUNT")
    private BigDecimal transferAmount;

    @Column(name = "PA_TRANSFER")
    private String paTransfer;

    @Column(name = "IBAN_TRANSFER")
    private String ibanTransfer;

    @Column(name = "IS_BOLLO")
    private Boolean isBollo;

    @Column(name = "STAZIONE")
    private Short stazione;

    @Column(name = "INTERMEDIARIO_PA")
    private Short intermediarioPa;

    @Column(name = "PSP")
    private Short psp;

    @Column(name = "CANALE")
    private Short canale;

    @Column(name = "INTERMEDIARIO_PSP")
    private Short intermediarioPsp;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "FAULT_CODE")
    private Short faultCode;

    @Column(name = "SESSION_ID")
    private String sessionId;

    @Column(name = "TIPO_EVENTO")
    private Short tipoEvento;

    @Column(name = "SOTTO_TIPO_EVENTO")
    private String sottoTipoEvento;

    @Column(name = "SERVICE_IDENTIFIER")
    private String serviceIdentifier;

}
