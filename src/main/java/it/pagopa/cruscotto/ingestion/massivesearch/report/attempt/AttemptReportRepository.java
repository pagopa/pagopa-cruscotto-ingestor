package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

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
 * Streaming JDBC access for the attempt report. For a batch of input keys it emits one
 * {@link AttemptReportRow} per token belonging to the matching debit positions, resolving
 * transfer counts / {@code HAS_BOLLO}, extra-info RRN/TID and {@code anag_*} labels.
 *
 * <p>The schema name is resolved from configuration ({@link DbSchemaConfig}); all input values are
 * bound as named parameters.</p>
 */
@Slf4j
@Repository
public class AttemptReportRepository {

    private static final String RRN_INFO_NAME = "rrn";
    private static final List<String> TID_INFO_NAMES =
        List.of("transactionId", "idTransaction", "pspTransactionId", "idPSPTransaction");

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;
    private final String baseSelect;

    public AttemptReportRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
        this.baseSelect = buildBaseSelect(this.schema);
    }

    /**
     * Streams the attempt (token) rows for the positions matching any key in the batch, issuing a
     * single set-based query.
     *
     * @param template the CSV template driving key resolution
     * @param keys     the batch of normalized input keys
     * @param window   optional temporal window limiting the analysed tokens
     * @param consumer receives every produced {@link AttemptReportRow}
     * @return the number of rows produced
     */
    public long streamByKeys(CsvTemplate template, List<SearchInputRow> keys, AnalysisWindow window, Consumer<AttemptReportRow> consumer) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String keyJoin = ReportKeyJoinSql.buildKeyJoin(template, schema, keys, params);
        if (keyJoin == null) {
            if (template == CsvTemplate.UNKNOWN) {
                log.warn("phase=REPORT_SKIP_BATCH report=attempt reason=unresolvable-template instanceId={} executionId={}",
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

    private AttemptReportRow mapRow(ResultSet rs) throws SQLException {
        List<String> values = new ArrayList<>(AttemptReportColumns.HEADERS.size());
        for (String column : AttemptReportColumns.HEADERS) {
            values.add(rs.getString(column));
        }
        return new AttemptReportRow(values);
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
            + " t.outcome AS outcome,"
            + " agg.token_count AS token_count,"
            + " encode(t.token, 'hex') AS token,"
            + " t.date_event AS date_born,"
            + " CASE WHEN t.outcome = 'OK' THEN t.payment_date END AS date_payed,"
            + " CASE WHEN t.outcome = 'OK' THEN 'true' ELSE 'false' END AS is_payed,"
            + " CASE WHEN t.id_carrello IS NOT NULL AND t.id_carrello <> '' THEN 'true' ELSE 'false' END AS is_cart,"
            + " t.touchpoint AS touchpoint,"
            + " t.payment_method AS payment_method,"
            + " trf.transfer_number AS transfer_number,"
            + " t.amount AS amount,"
            + " psp.codice AS psp,"
            + " ipsp.codice AS broker_psp,"
            + " ipa.codice AS broker_pa,"
            + " st.codice AS station,"
            + " ch.codice AS channel,"
            + " t.fee AS fee,"
            + " xi.rrn AS add_info_rrn,"
            + " xi.tid AS add_info_tid,"
            + " pae.description AS label_pa,"
            + " psp.description AS label_psp,"
            + " ipa.description AS label_broker_pa,"
            + " ipsp.description AS label_broker_psp,"
            + " t.touchpoint AS label_touchpoint,"
            + " t.payment_method AS label_payment_method,"
            + " CASE WHEN trf.bollo_count > 0 THEN 'true' ELSE 'false' END AS has_bollo"
            + " FROM " + position + " p"
            + " JOIN " + tokens + " t ON t.fk_position = p.id" + win("t")
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS token_count FROM " + tokens + " tks WHERE tks.fk_position = p.id" + win("tks")
            + " ) agg ON TRUE"
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS transfer_number,"
            + "          COUNT(*) FILTER (WHERE tr.is_bollo) AS bollo_count"
            + "   FROM " + transfers + " tr WHERE tr.fk_token = t.id"
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
