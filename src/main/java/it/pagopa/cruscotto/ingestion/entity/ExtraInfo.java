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

@Entity
@Table(name = "EXTRA_INFO")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExtraInfo {

    @Id
    @Column(name = "ID")
    private Long id;

    @Column(name = "DATE_EVENT")
    private LocalDate dateEvent;

    @Column(name = "FK_TOKENS")
    private Long fkTokens;

    @Column(name = "INFO_NAME", length = 30)
    private String infoName;

    @Column(name = "INFO_VALUE", length = 40)
    private String infoValue;

    @Column(name = "TIPO_EVENTO")
    private Short tipoEvento;

}
