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

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "EXTRA_INFO")
public class ExtraInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_extra_info")
    @SequenceGenerator(name = "seq_extra_info", sequenceName = "SQ_EXTRA_INFO", allocationSize = 1)
    @Column(name = "ID")
    private Integer id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_TOKEN")
    private Integer fkToken;

    @Column(name = "INFO_NAME")
    private String infoName;

    @Column(name = "INFO_VALUE")
    private String infoValue;

    @Column(name = "TIPO_EVENTO")
    private Short tipoEvento;
}
