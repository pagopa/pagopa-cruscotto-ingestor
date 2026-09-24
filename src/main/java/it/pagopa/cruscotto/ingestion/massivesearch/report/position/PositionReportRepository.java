package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportKeyJoinSql;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportWindowSql;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
 * rappresentativo (ultimo disponibile) serve solo per DATE_BORN e per gli identificativi
 * IUV/CREDITOR_REF_ID/IS_CART. The schema name is resolved from configuration
 * ({@link DbSchemaConfig}); all input values are bound as named parameters.</p>
 */
@Slf4j
@Repository
public class PositionReportRepository {

    private static final String RRN_INFO_NAME = "rrn";
    private static final List<String> TID_INFO_NAMES =
        List.of("transactionId", "idTransaction", "pspTransactionId", "idPSPTransaction");

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;
    private final String baseSelect;

    public PositionReportRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
        this.baseSelect = buildBaseSelect(this.schema);
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
        params.addValue("winFrom", window.fromInclusive());
        params.addValue("winTo", window.toExclusive());
        String sql = baseSelect + " " + keyJoin;
        AtomicLong rows = new AtomicLong();
        jdbc.query(sql, params, rs -> {
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

    private String buildBaseSelect(String schema) {
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
            + " p.nav AS nav,"
            + " p.pa_emittente AS pa,"
            + " t.iuv AS iuv,"
            + " t.creditor_ref_id AS creditor_ref_id,"
            + " agg.token_count AS token_count,"
            + " CASE WHEN agg.is_payed THEN 'INCASSATO' ELSE 'PAGABILE' END AS outcome,"
            + " t.date_event AS date_born,"
            + " agg.date_payed AS date_payed,"
            + " CASE WHEN agg.is_payed THEN 'true' ELSE 'false' END AS is_payed,"
            + " CASE WHEN t.id_carrello IS NOT NULL AND t.id_carrello <> '' THEN 'true' ELSE 'false' END AS is_cart,"
            // Campi "in riferimento al token OK" (spec): valorizzati solo se la posizione ha un token
            // OK (agg.is_payed). Senza token OK il token rappresentativo t e' l'ultimo disponibile e
            // serve solo per DATE_BORN/IUV/CREDITOR_REF_ID/IS_CART: qui questi campi restano vuoti.
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
            + " FROM " + position + " p"
            + " JOIN LATERAL ("
            + "   SELECT tk.* FROM " + tokens + " tk"
            + "   WHERE tk.fk_position = p.id" + win("tk")
            + "   ORDER BY (CASE WHEN tk.outcome = 'OK' THEN 0 ELSE 1 END),"
            + "            CASE WHEN tk.outcome = 'OK' THEN tk.payment_date END ASC NULLS LAST,"
            + "            tk.payment_date DESC NULLS LAST, tk.id DESC"
            + "   LIMIT 1"
            + " ) t ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS token_count,"
            + "          MIN(tks.payment_date) FILTER (WHERE tks.outcome = 'OK') AS date_payed,"
            + "          BOOL_OR(tks.outcome = 'OK') AS is_payed"
            + "   FROM " + tokens + " tks WHERE tks.fk_position = p.id" + win("tks")
            + " ) agg ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS transfer_number FROM " + transfers + " tr WHERE tr.fk_token = t.id"
            + " ) trf ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT MAX(ei.info_value) FILTER (WHERE ei.info_name = '" + RRN_INFO_NAME + "') AS rrn,"
            + "          MAX(ei.info_value) FILTER (WHERE ei.info_name IN (" + tidInList + ")) AS tid"
            + "   FROM " + extraInfo + " ei WHERE ei.fk_token = t.id"
            + " ) xi ON TRUE"
            + " LEFT JOIN " + anagPsp + " psp ON psp.id = t.psp"
            + " LEFT JOIN " + anagIntPsp + " ipsp ON ipsp.id = t.intermediario_psp"
            + " LEFT JOIN " + anagIntPa + " ipa ON ipa.id = t.intermediario_pa"
            + " LEFT JOIN " + anagStazione + " st ON st.id = t.stazione"
            + " LEFT JOIN " + anagCanale + " ch ON ch.id = t.canale"
            + " LEFT JOIN " + anagPaEmittente + " pae ON pae.codice = p.pa_emittente";
    }

    /**
     * Optional temporal window predicate on {@code payment_date} for the given token alias.
     * Delegates to the shared {@link ReportWindowSql}.
     */
    private static String win(String alias) {
        return ReportWindowSql.paymentDateWindow(alias);
    }
}
