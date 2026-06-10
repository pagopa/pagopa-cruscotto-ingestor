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

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "POSITION_TRANSFERS")
public class PositionTransfers {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_position_transfers")
    @SequenceGenerator(name = "seq_position_transfers", sequenceName = "SQ_POSITION_TRANSFERS", allocationSize = 1)
    @Column(name = "ID")
    private Integer id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_TOKEN")
    private Integer fkToken;

    @Column(name = "PA_TRANSFER")
    private String paTransfer;

    @Column(name = "ID_TRANSFER")
    private Short idTransfer;

    @Column(name = "IBAN_TRANSFER")
    private String ibanTransfer;

    @Column(name = "AMOUNT_TRANSFER")
    private BigDecimal amountTransfer;

    @Column(name = "IS_BOLLO")
    private Boolean isBollo;

    @Column(name = "PSP")
    private Short psp;

    @Column(name = "INTERMEDIARIO_PSP")
    private Short intermediarioPsp;

    @Column(name = "CANALE")
    private Short canale;
}
