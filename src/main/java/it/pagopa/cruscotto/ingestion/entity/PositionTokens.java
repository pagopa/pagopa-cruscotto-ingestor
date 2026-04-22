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
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "POSITION_TOKENS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PositionTokens {

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

    @Column(name = "AMOUNT")
    private BigDecimal amount;

    @Column(name = "FEE")
    private BigDecimal fee;

    @Column(name = "ID_CARRELLO")
    private String idCarrello;

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

    @Column(name = "TOUCHPOINT")
    private String touchpoint;

    @Column(name = "PAYMENT_METHOD")
    private String paymentMethod;

    @Column(name = "SESSION_ID")
    private String sessionId;

    @Column(name = "TIPO_EVENTO")
    private String tipoEvento;

    @Column(name = "SOTTO_TIPO_EVENTO")
    private String sottoTipoEvento;

    @Column(name = "SERVICE_IDENTIFIER")
    private String serviceIdentifier;

}
