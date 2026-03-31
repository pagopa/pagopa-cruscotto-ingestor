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

@Entity
@Table(name = "POSITION_TRANSFERS")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PositionTransfers {

    @Id
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_TOKEN")
    private Long fkToken;

    @Column(name = "PA_TRANSFER")
    private byte[] paTransfer;

    @Column(name = "ID_TRANSFER")
    private BigDecimal idTransfer;

    @Column(name = "IBAN_TRANSFER")
    private String ibanTransfer;

    @Column(name = "AMOUNT_TRANSFER", length = 35)
    private String amountTransfer;

    @Column(name = "IS_BOLLO")
    private Boolean isBollo;

}
