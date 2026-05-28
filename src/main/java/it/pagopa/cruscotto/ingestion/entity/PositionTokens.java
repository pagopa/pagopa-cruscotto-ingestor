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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "POSITION_TOKENS")
public class PositionTokens {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_position_tokens")
    @SequenceGenerator(name = "seq_position_tokens", sequenceName = "SQ_POSITION_TOKENS", allocationSize = 1)
    @Column(name = "ID")
    private Integer id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_POSITION")
    private Integer fkPosition;

    @Column(name = "TOKEN")
    private byte[] token;

    @Column(name = "AMOUNT")
    private BigDecimal amount;

    @Column(name = "FEE")
    private BigDecimal fee;

    @Column(name = "IUV")
    private String iuv;

    @Column(name = "CREDITOR_REF_ID")
    private String creditorRefId;

    @Column(name = "OUTCOME")
    private String outcome;

    @Column(name = "ID_CARRELLO")
    private String idCarrello;

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
