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

@Entity
@Table(name = "EXTRA_INFO")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExtraInfo {

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

    @Column(name = "TOKEN")
    private String token;

    @Column(name = "INTERMEDIARIO_PSP")
    private String intermediarioPsp;

    @Column(name = "PSP")
    private String psp;

    @Column(name = "CANALE")
    private String canale;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "PAYMENT_METHOD")
    private String paymentMethod;

    @Column(name = "TRANSACTION_STATUS")
    private String transactionStatus;

    @Column(name = "ADDITIONAL_INFO")
    private String additionalInfo;

    @Column(name = "SESSION_ID")
    private String sessionId;

    @Column(name = "TIPO_EVENTO")
    private String tipoEvento;

    @Column(name = "SOTTO_TIPO_EVENTO")
    private String sottoTipoEvento;

    @Column(name = "SERVICE_IDENTIFIER")
    private String serviceIdentifier;

}
