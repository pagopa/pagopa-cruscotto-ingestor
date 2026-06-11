package it.pagopa.cruscotto.ingestion.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "DATALAYER_EVENTS_WF")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataLayerEventsWf {

    @Id
    @Column(name = "UNIQUE_ID")
    private String uniqueId;

    @Column(name = "INSERTED_TIMESTAMP")
    private LocalDateTime insertedTimestamp;

    @Column(name = "IS_EVENT_MULTI_PAYMENT")
    private Boolean isEventMultiPayment;

    @Column(name = "NAV", length = 50)
    private String nav;

    @Column(name = "PA_EMITTENTE", length = 50)
    private String paEmittente;

    @Column(name = "IUV", length = 35)
    private String iuv;

    @Column(name = "CREDITOR_REF_ID")
    private String creditorRefId;

    @Column(name = "TOKEN")
    private String token;

    @Column(name = "PSP")
    private Short psp;

    @Column(name = "INTERMEDIARIO_PSP")
    private Short intermediarioPsp;

    @Column(name = "INTERMEDIARIO_PA")
    private Short intermediarioPa;

    @Column(name = "CANALE")
    private Short canale;

    @Column(name = "STAZIONE")
    private Short stazione;

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

    @Column(name = "PAYMENT_METHOD")
    private String paymentMethod;

}
