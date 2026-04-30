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

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "ANAG_FAULT_CODE")
public class AnagFaultCode {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_anag_fault_code")
    @SequenceGenerator(name = "seq_anag_fault_code", sequenceName = "SQ_ANAG_FAULT_CODE", allocationSize = 1)
    @Column(name = "ID")
    private Short id;

    @Column(name = "CODICE")
    private String codice;
}
