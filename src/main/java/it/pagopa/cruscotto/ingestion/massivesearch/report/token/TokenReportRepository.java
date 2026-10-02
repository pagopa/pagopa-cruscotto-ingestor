package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

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
 * Streaming JDBC access for the token report. For a batch of input keys it emits one
 * {@link TokenReportRow} per token belonging to the matching debit positions, resolving
 * transfer counts / {@code HAS_BOLLO}, extra-info RRN/TID and {@code anag_*} labels.
 *
 * <p>The schema name is resolved from configuration ({@link DbSchemaConfig}); all input values are
 * bound as named parameters.</p>
 *
 * <p>Semantics: {@code DATE_BORN} is the ADX {@code INSERTED_TIMESTAMP} of the
 * {@code activatePaymentNotice(V2)} event stored on the token (formatted {@code yyyy-MM-dd});
 * {@code DATE_PAYED} is the token {@code PAYMENT_DATE}, written once from the first
 * {@code sendPaymentOutcome} with {@code OUTCOME_REQ = 'OK'};
 * {@code ADD_INFO_RRN}/{@code ADD_INFO_TID} are exposed only for tokens with {@code OUTCOME = 'OK'}.</p>
 *
 * <p>{@code TOKEN_COUNT} e' <strong>overall</strong>: conta tutti i tentativi della posizione
 * presenti a sistema (retention online), indipendentemente dalla finestra di analisi. La finestra
 * limita quali token/transfer compaiono come righe, non il conteggio.</p>
 */
@Slf4j
@Repository
public class TokenReportRepository {

    private static final String RRN_INFO_NAME = "rrn";
    private static final List<String> TID_INFO_NAMES =
        List.of("transactionId", "idTransaction", "pspTransactionId", "idPSPTransaction");

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public TokenReportRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * Streams the token rows for the positions matching any key in the batch, issuing a
     * single set-based query.
     *
     * @param template the CSV template driving key resolution
     * @param keys     the batch of normalized input keys
     * @param window   optional temporal window limiting the analysed tokens
     * @param consumer receives every produced {@link TokenReportRow}
     * @return the number of rows produced
     */
    public long streamByKeys(CsvTemplate template, List<SearchInputRow> keys, AnalysisWindow window, Consumer<TokenReportRow> consumer) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String keyJoin = ReportKeyJoinSql.buildKeyJoin(template, schema, keys, params);
        if (keyJoin == null) {
            if (template == CsvTemplate.UNKNOWN) {
                log.warn("phase=REPORT_SKIP_BATCH report=token reason=unresolvable-template instanceId={} executionId={}",
                    MDC.get("instanceId"), MDC.get("executionId"));
            }
            return 0L;
        }
        ReportWindowSql.bind(params, window);
        String sql = buildBaseSelect(schema, window) + " " + keyJoin;
        AtomicLong rows = new AtomicLong();
        jdbc.query(sql, params, rs -> {
            consumer.accept(mapRow(rs));
            rows.incrementAndGet();
        });
        return rows.get();
    }

    private TokenReportRow mapRow(ResultSet rs) throws SQLException {
        List<String> values = new ArrayList<>(TokenReportColumns.HEADERS.size());
        for (String column : TokenReportColumns.HEADERS) {
            values.add(rs.getString(column));
        }
        return new TokenReportRow(values);
    }

    /** Package-private per consentire ai test di verificare la semantica dell'SQL generato. */
    String buildBaseSelect(String schema, AnalysisWindow window) {
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
            + " convert_from(t.token, 'UTF8') AS token,"
            + " to_char(t.inserted_timestamp, 'YYYY-MM-DD') AS date_born,"
            + " t.payment_date AS date_payed,"
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
            + " CASE WHEN t.outcome = 'OK' THEN xi.rrn END AS add_info_rrn,"
            + " CASE WHEN t.outcome = 'OK' THEN xi.tid END AS add_info_tid,"
            + " pae.description AS label_pa,"
            + " psp.description AS label_psp,"
            + " ipa.description AS label_broker_pa,"
            + " ipsp.description AS label_broker_psp,"
            + " t.touchpoint AS label_touchpoint,"
            + " t.payment_method AS label_payment_method,"
            + " CASE WHEN trf.bollo_count > 0 THEN 'true' ELSE 'false' END AS has_bollo"
            + " FROM " + position + " p"
            + " JOIN " + tokens + " t ON t.fk_position = p.id" + win("t", window)
            // TOKEN_COUNT e' "overall" per spec: conta TUTTI i tentativi della posizione presenti a
            // sistema (retention online), non solo quelli che cadono nella finestra di analisi.
            // Deliberatamente senza win(): non aggiungere qui il predicato temporale.
            + " LEFT JOIN LATERAL ("
            + "   SELECT COUNT(*) AS token_count FROM " + tokens + " tks WHERE tks.fk_position = p.id"
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
     * Optional temporal window predicate on {@code inserted_timestamp} for the given token alias.
     * Delegates to the shared {@link ReportWindowSql}.
     */
    private static String win(String alias, AnalysisWindow window) {
        return ReportWindowSql.tokenWindow(alias, window);
    }
}
