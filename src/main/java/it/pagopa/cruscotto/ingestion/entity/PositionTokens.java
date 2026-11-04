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

    /**
     * Riferimento LIBERO scritto dall'Ente Creditore quando chiama l'API carrello. Non e' un
     * identificativo: non ha naming convention, non e' univoco, e viene riusato — un EC ha usato
     * il valore costante {@code "pagamentoOnline"} su 2.069 token di 1.125 pagamenti diversi in
     * due giorni. <strong>Non usarlo per dedurre se un pagamento e' un carrello</strong>: per
     * quello c'e' {@link #isCart}.
     */
    @Column(name = "ID_CARRELLO")
    private String idCarrello;

    /**
     * Il token fa parte di un pagamento multiplo: N avvisi pagati in un'unica transazione con
     * <strong>una sola commissione</strong>, che viene riportata su tutti gli N token. Questo flag
     * e' l'unico segnale che dice a chi legge l'export che la fee e' condivisa e non va sommata.
     *
     * <p>Popolato da {@code PositionEventUpdateService} dal percorso eventi: un
     * {@code EVENT_ID_REQ} di {@code closePayment-v2} presente su piu' token marca tutti i suoi
     * token.</p>
     *
     * <p><strong>Solo {@code true} viene mai scritto.</strong> {@code null} vale "non e' un
     * carrello": scrivere {@code false} esplicitamente costringerebbe a un UPDATE su ogni token
     * che riceve un evento, anche quando non e' cambiato nulla, e la distinzione fra "accertato
     * singolo" e "non ancora noto" non ha consumatori — i report rendono i due casi allo stesso
     * modo.</p>
     */
    @Column(name = "IS_CART")
    private Boolean isCart;

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

    /**
     * Timestamp della riga sorgente ADX (INSERTED_TIMESTAMP) del PRIMO evento dello stream TOKEN:
     * l'insert e' registry-gated e l'UPDATE non riscrive questa colonna (first-write-wins).
     * Colonna passiva per analisi; vale l'invariante DATE_EVENT = date(INSERTED_TIMESTAMP).
     */
    @Column(name = "INSERTED_TIMESTAMP")
    private LocalDateTime insertedTimestamp;
}
