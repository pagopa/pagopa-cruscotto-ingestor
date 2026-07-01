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
@Table(name = "ANAG_PA_EMITTENTE")
public class AnagPaEmittente {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_anag_pa_emittente")
    @SequenceGenerator(name = "seq_anag_pa_emittente", sequenceName = "SQ_ANAG_PA_EMITTENTE", allocationSize = 1)
    @Column(name = "ID")
    private Short id;

    @Column(name = "CODICE")
    private String codice;

    @Column(name = "DESCRIPTION")
    private String description;
}
