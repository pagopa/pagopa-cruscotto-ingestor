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
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_POSITION")
    private Long fkPosition;

    @Column(name = "TOKEN")
    private byte[] token;

    @Column(name = "AMOUNT")
    private BigDecimal amount;

    @Column(name = "FEE")
    private BigDecimal fee;

    @Column(name = "IUV", length = 35)
    private String iuv;

    @Column(name = "CREDITOR_REF_ID", length = 35)
    private String creditorRefId;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "ID_CARRELLO")
    private Short idCarrello;

    @Column(name = "STAZIONE")
    private Short stazione;

    @Column(name = "CANALE")
    private Short canale;

    @Column(name = "INTERMEDIARIO_PA")
    private Short intermediarioPa;

    @Column(name = "INTERMEDIARIO_PSP")
    private Short intermediarioPsp;

    @Column(name = "PSP")
    private Short psp;

    @Column(name = "TOUCHPOINT")
    private String touchpoint;

    @Column(name = "PAYMENT_METHOD")
    private String paymentMethod;

    @Column(name = "PAYMENT_DATE")
    private LocalDateTime paymentDate;

}
