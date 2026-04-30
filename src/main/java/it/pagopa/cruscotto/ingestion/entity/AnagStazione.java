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
@Table(name = "ANAG_STAZIONE")
public class AnagStazione {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_anag_stazione")
    @SequenceGenerator(name = "seq_anag_stazione", sequenceName = "SQ_ANAG_STAZIONE", allocationSize = 1)
    @Column(name = "ID")
    private Short id;

    @Column(name = "CODICE")
    private String codice;
}
