package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportKeyJoinSql;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportQueryExecutor;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportWindowSql;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Streaming JDBC access for the position report. For a batch of input keys it emits one
 * {@link PositionReportRow} per matching debit position, aggregating token / transfer / extra-info
 * data and resolving {@code anag_*} labels.
 *
 * <p>The representative token of a position is the one that closed it positively ({@code OUTCOME='OK'})
 * or, when none did, the most recent token. Quando la posizione non ha alcun token OK, i campi
 * "in riferimento al token OK" (TOKEN, TOUCHPOINT, PAYMENT_METHOD, AMOUNT, PSP, BROKER_*, STATION,
 * CHANNEL, FEE, TRANSFER_NUMBER, ADD_INFO_*, LABEL_* eccetto LABEL_PA) restano vuoti; il token
 * rappresentativo (ultimo disponibile) serve per DATE_BORN e per i campi neutri IUV,
 * CREDITOR_REF_ID e IS_CART, che sono sempre valorizzati. The schema name is resolved from configuration
 * ({@link DbSchemaConfig}); all input values are bound as named parameters.</p>
 *
 * <p>{@code DATE_BORN} is the ADX {@code INSERTED_TIMESTAMP} of the {@code activatePaymentNotice(V2)}
 * event stored on the representative token, formatted {@code yyyy-MM-dd}.</p>
 *
 * <p>{@code TOKEN_COUNT} e' <strong>overall</strong>: conta tutti i tentativi della posizione
 * presenti a sistema (retention online), indipendentemente dalla finestra di analisi. Gli altri
 * aggregati ({@code DATE_PAYED}, {@code IS_PAYED}) restano relativi alla finestra.</p>
 */
@Slf4j
@Repository
public class PositionReportRepository {

    private static final String RRN_INFO_NAME = "rrn";
    private static final List<String> TID_INFO_NAMES =
        List.of("transactionId", "idTransaction", "pspTransactionId", "idPSPTransaction");

    private final ReportQueryExecutor queryExecutor;
    private final String schema;
    private final int childMarginDays;

    public PositionReportRepository(ReportQueryExecutor queryExecutor, DbSchemaConfig dbSchemaConfig,
                                    MassiveSearchProperties properties) {
        this.queryExecutor = queryExecutor;
        this.schema = dbSchemaConfig.getSchemaName();
        this.childMarginDays = properties.getExecution().getChildDateMarginDays();
    }

    /**
     * Streams the position rows matching any key in the given batch, issuing a single set-based query.
     *
     * @param template the CSV template driving key resolution
     * @param keys     the batch of normalized input keys
     * @param window   optional temporal window limiting the analysed tokens
     * @param consumer receives every produced {@link PositionReportRow}
     * @return the number of rows produced
     */
    public long streamByKeys(CsvTemplate template, List<SearchInputRow> keys, AnalysisWindow window, Consumer<PositionReportRow> consumer) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String keyJoin = ReportKeyJoinSql.buildKeyJoin(template, schema, keys, params);
        if (keyJoin == null) {
            if (template == CsvTemplate.UNKNOWN) {
                log.warn("phase=REPORT_SKIP_BATCH report=position reason=unresolvable-template instanceId={} executionId={}",
                    MDC.get("instanceId"), MDC.get("executionId"));
            }
            return 0L;
        }
        ReportWindowSql.bind(params, window);
        ReportWindowSql.bindChildMargin(params, childMarginDays);
        String sql = buildBaseSelect(schema, window, keyJoin);
        AtomicLong rows = new AtomicLong();
        queryExecutor.stream(sql, params, rs -> {
            consumer.accept(mapRow(rs));
            rows.incrementAndGet();
        });
        return rows.get();
    }

    private PositionReportRow mapRow(ResultSet rs) throws SQLException {
        List<String> values = new ArrayList<>(PositionReportColumns.HEADERS.size());
        for (String column : PositionReportColumns.HEADERS) {
            values.add(rs.getString(column));
        }
        return new PositionReportRow(values);
    }

    /** Package-private per consentire ai test di verificare la semantica dell'SQL generato. */
    String buildBaseSelect(String schema, AnalysisWindow window, String keyJoin) {
        String position = schema + ".position";
        String tokens = schema + ".position_tokens";
        String transfers = schema + ".position_transfers";
        String extraInfo = schema + ".extra_info";
        String anagPsp = schema + ".anag_psp";
        String anagIntPsp = schema + ".anag_intermediario_psp";
        String anagIntPa = schema + ".anag_intermediario_pa";
        String anagStazione = schema + ".anag_stazione";
        String anagCanale = schema + ".anag_canale";
        String anagPaEmittente = schema + ".anag_pa_emittente";

        String tidInList = "'" + String.join("','", TID_INFO_NAMES) + "'";

        return "SELECT"
            + " pk.nav AS nav,"
            + " pk.pa AS pa,"
            + " t.iuv AS iuv,"
            + " t.creditor_ref_id AS creditor_ref_id,"
            + " agg.token_count AS token_count,"
            + " CASE WHEN agg.is_payed THEN 'INCASSATO' ELSE 'PAGABILE' END AS outcome,"
            + " to_char(t.inserted_timestamp, 'YYYY-MM-DD') AS date_born,"
            + " agg.date_payed AS date_payed,"
            + " CASE WHEN agg.is_payed THEN 'true' ELSE 'false' END AS is_payed,"
            + " CASE WHEN t.id_carrello IS NOT NULL AND t.id_carrello <> '' THEN 'true' ELSE 'false' END AS is_cart,"
            // Campi "in riferimento al token OK" (spec): valorizzati solo se la posizione ha un token
            // OK (agg.is_payed). Senza token OK il token rappresentativo t e' l'ultimo disponibile e
            // alimenta solo i campi neutri (DATE_BORN, IUV, CREDITOR_REF_ID, IS_CART).
            + " CASE WHEN agg.is_payed THEN convert_from(t.token, 'UTF8') END AS token,"
            + " CASE WHEN agg.is_payed THEN t.touchpoint END AS touchpoint,"
            + " CASE WHEN agg.is_payed THEN t.payment_method END AS payment_method,"
            + " CASE WHEN agg.is_payed THEN trf.transfer_number END AS transfer_number,"
            + " CASE WHEN agg.is_payed THEN t.amount END AS amount,"
            + " CASE WHEN agg.is_payed THEN psp.codice END AS psp,"
            + " CASE WHEN agg.is_payed THEN ipsp.codice END AS broker_psp,"
            + " CASE WHEN agg.is_payed THEN ipa.codice END AS broker_pa,"
            + " CASE WHEN agg.is_payed THEN st.codice END AS station,"
            + " CASE WHEN agg.is_payed THEN ch.codice END AS channel,"
            + " CASE WHEN agg.is_payed THEN t.fee END AS fee,"
            + " CASE WHEN agg.is_payed THEN xi.rrn END AS add_info_rrn,"
            + " CASE WHEN agg.is_payed THEN xi.tid END AS add_info_tid,"
            + " pae.description AS label_pa,"
            + " CASE WHEN agg.is_payed THEN psp.description END AS label_psp,"
            + " CASE WHEN agg.is_payed THEN ipa.description END AS label_broker_pa,"
            + " CASE WHEN agg.is_payed THEN ipsp.description END AS label_broker_psp,"
            + " CASE WHEN agg.is_payed THEN t.touchpoint END AS label_touchpoint,"
            + " CASE WHEN agg.is_payed THEN t.payment_method END AS label_payment_method"
            // Una riga per POSIZIONE, cioe' per business key: la stessa (NAV, EC) puo' avere piu'
            // occorrenze in POSITION (ADX ne crea una nuova a ogni evento, es. emessa a gennaio e
            // pagata a marzo) e il report deve emetterne una sola. La riduzione a chiavi distinte sta
            // in subquery, prima delle LATERAL, cosi' queste girano una volta per chiave invece che
            // una volta per occorrenza. Dalla posizione si prendono solo NAV e PA_EMITTENTE: tutto il
            // resto viene da token/transfer, quindi quale occorrenza "vinca" e' indifferente.
            + " FROM (SELECT DISTINCT p.nav AS nav, p.pa_emittente AS pa"
            + "   FROM " + position + " p " + keyJoin
            + "   WHERE TRUE" + ReportWindowSql.positionWindow("p", window)
            + " ) pk"
            // Token rappresentativo: quello incassato, altrimenti l'ultimo disponibile (spec DATE_BORN).
            // LEFT e non INNER: una posizione senza alcun tentativo deve comparire con i campi del
            // token vuoti (requisito cliente), non sparire dal report.
            + " LEFT JOIN LATERAL ("
            // parent_last_date: ultima data che ha toccato la posizione del token (nascita + date_events).
            // Serve come estremo superiore esatto ai figli del token, che qui non hanno p2 in scope.
            + "   SELECT tk.*, " + ReportWindowSql.positionLastDate("p2") + " AS parent_last_date"
            + "   FROM " + tokens + " tk"
            + "   JOIN " + position + " p2 ON p2.id = tk.fk_position"
            + "   WHERE " + sameKey("p2", "pk") + child("tk", "p2")
            + "   ORDER BY (CASE WHEN tk.outcome = 'OK' THEN 0 ELSE 1 END),"
            + "            CASE WHEN tk.outcome = 'OK' THEN tk.payment_date END ASC NULLS LAST,"
            + "            tk.payment_date DESC NULLS LAST, tk.inserted_timestamp DESC NULLS LAST, tk.id DESC"
            + "   LIMIT 1"
            + " ) t ON TRUE"
            + " LEFT JOIN LATERAL ("
            // Un'unica scansione per tutti e tre gli aggregati sui tentativi: hanno esattamente la
            // stessa FROM/WHERE, tenerli separati raddoppiava le probe su position senza alcun
            // guadagno. TOKEN_COUNT e' "overall" per spec: conta TUTTI i tentativi della posizione
            // presenti a sistema (retention online), su tutte le occorrenze della business key e
            // senza alcun vincolo di periodo. Deliberatamente senza finestra: non aggiungere qui un
            // predicato temporale sulla ricerca.
            //
            // DATE_PAYED resta un MIN non filtrato, allineato ai report Token e Transfer. Sotto
            // l'invariante PAYMENT_DATE IS NOT NULL <=> OUTCOME='OK' le due forme coincidono;
            // l'invariante e' pero' garantita solo a livello di singolo evento SPO. Una sequenza
            // OUTCOME_REQ='OK' seguita da OUTCOME_REQ='KO' sullo stesso token degrada OUTCOME
            // lasciando PAYMENT_DATE (first-write-wins), e in quel caso DATE_PAYED resta valorizzata
            // con IS_PAYED='false'. E' una scelta deliberata di coerenza fra i tre report, non una
            // svista: filtrare qui su OUTCOME disallineerebbe Position dagli altri due.
            + "   SELECT MIN(tks.payment_date) AS date_payed,"
            + "          BOOL_OR(tks.outcome = 'OK') AS is_payed,"
            + "          COUNT(*) AS token_count"
            + "   FROM " + tokens + " tks"
            + "   JOIN " + position + " p2 ON p2.id = tks.fk_position"
            + "   WHERE " + sameKey("p2", "pk") + child("tks", "p2")
            + " ) agg ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS transfer_number FROM " + transfers + " tr WHERE tr.fk_token = t.id" + childOfToken("tr")
            + " ) trf ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT MAX(ei.info_value) FILTER (WHERE ei.info_name = '" + RRN_INFO_NAME + "') AS rrn,"
            + "          MAX(ei.info_value) FILTER (WHERE ei.info_name IN (" + tidInList + ")) AS tid"
            + "   FROM " + extraInfo + " ei WHERE ei.fk_token = t.id" + childOfToken("ei")
            + " ) xi ON TRUE"
            + " LEFT JOIN " + anagPsp + " psp ON psp.id = t.psp"
            + " LEFT JOIN " + anagIntPsp + " ipsp ON ipsp.id = t.intermediario_psp"
            + " LEFT JOIN " + anagIntPa + " ipa ON ipa.id = t.intermediario_pa"
            + " LEFT JOIN " + anagStazione + " st ON st.id = t.stazione"
            + " LEFT JOIN " + anagCanale + " ch ON ch.id = t.canale"
            + " LEFT JOIN " + anagPaEmittente + " pae ON pae.codice = pk.pa";
    }

    /** Correla un'occorrenza di {@code position} alla business key del report. */
    private static String sameKey(String positionAlias, String keyAlias) {
        return positionAlias + ".nav = " + keyAlias + ".nav AND "
            + positionAlias + ".pa_emittente = " + keyAlias + ".pa";
    }


    /**
     * Bound correlato che consente il partition pruning sulle tabelle correlate per {@code date_event}.
     * Delega a {@link ReportWindowSql#childOfToken}.
     *
     * <p>Usato in due direzioni: figli del token ({@code position_transfers}, {@code extra_info}) e
     * token rispetto alla propria occorrenza di {@code position}. Quest'ultima relazione e' garantita
     * dall'ingestion, che associa un token solo a una posizione creata nelle 24 ore precedenti
     * ({@code PositionRepository#findLatestByBusinessKeyWithin24h}): un pagamento a mesi di distanza
     * genera una <em>nuova</em> occorrenza di posizione, non un token lontano dalla propria.</p>
     */
    private String child(String childAlias, String parentAlias) {
        return ReportWindowSql.childOfToken(childAlias, parentAlias, childMarginDays);
    }

    /**
     * Bound per le tabelle figlie del token ({@code position_transfers}, {@code extra_info}), con
     * estremo superiore <strong>esatto</strong>: l'ultima data che ha toccato la posizione del token,
     * proiettata dalla LATERAL come {@code parent_last_date}.
     *
     * <p>Un margine fisso di pochi giorni escluderebbe le righe create da una
     * {@code sendPaymentOutcome} tardiva — tipicamente le {@code extra_info} con RRN/TID, che portano
     * la data dell'evento tardivo ma {@code fk_token} del token originale. Qui non e' possibile usare
     * {@code p2} direttamente: nelle LATERAL dei figli non e' in scope.</p>
     */
    private String childOfToken(String childAlias) {
        return ReportWindowSql.childOfTokenUpTo(childAlias, "t", "t.parent_last_date", childMarginDays);
    }
}
