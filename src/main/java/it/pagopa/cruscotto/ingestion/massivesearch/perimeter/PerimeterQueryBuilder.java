package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the dynamic SQL that resolves a {@link PerimeterFilter} into distinct {@code NAV;EC}
 * pairs over the existing SERT tables ({@code position} joined to {@code position_tokens}).
 *
 * <p>The schema name is resolved from configuration ({@link DbSchemaConfig}); it is never hardcoded.
 * All user-supplied values are bound as named parameters to avoid SQL injection.</p>
 */
@Slf4j
@Component
public class PerimeterQueryBuilder {

    private final String schema;

    public PerimeterQueryBuilder(DbSchemaConfig dbSchemaConfig) {
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * Builds the perimeter query for the given filter.
     *
     * @param filter deserialized filter definition (may be empty but not {@code null})
     * @return the SQL and bound parameters producing distinct {@code (pa, nav)} pairs
     */
    public PerimeterQuery build(PerimeterFilter filter) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        List<String> conditions = new ArrayList<>();

        conditions.add("p.nav IS NOT NULL");
        conditions.add("p.pa_emittente IS NOT NULL");

        appendPaymentPeriod(filter.getPaymentPeriod(), conditions, params);
        appendPaymentStatuses(filter.getPaymentStatuses(), conditions, params);
        appendInStrings("t.touchpoint", "touchpoints", filter.getTouchpoints(), conditions, params);
        appendInStrings("t.payment_method", "paymentMethods", filter.getPaymentMethods(), conditions, params);
        appendAmount(filter.getAmount(), conditions, params);
        appendCreditors(filter.getCreditors(), conditions, params);
        appendInIntegers("t.psp", "psps", filter.getPsps(), conditions, params);
        appendTechnologicalPartners(filter.getTechnologicalPartners(), conditions, params);
        appendInIntegers("t.canale", "channels", filter.getChannels(), conditions, params);
        appendInIntegers("t.stazione", "stations", filter.getStations(), conditions, params);

        String sql = "SELECT DISTINCT p.pa_emittente AS pa, p.nav AS nav"
            + " FROM " + schema + ".position p"
            + " JOIN " + schema + ".position_tokens t ON t.fk_position = p.id"
            + " WHERE " + String.join(" AND ", conditions)
            + " ORDER BY pa, nav";

        log.debug("Built perimeter query with {} condition(s)", conditions.size());
        return new PerimeterQuery(sql, params);
    }

    private void appendPaymentPeriod(PerimeterFilter.PaymentPeriod period, List<String> conditions, MapSqlParameterSource params) {
        if (period == null) {
            return;
        }
        // Finestra su inserted_timestamp (sorgente ADX, sempre valorizzato) e non su payment_date, che
        // e' null per i token non pagati: stessa colonna e stessi bound di ReportWindowSql.
        // Date assolute: bind di LocalDateTime, nessuna conversione tz.
        //
        // NOTA: qui vengono applicati solo i bound indicati dall'utente. Il bound inferiore di default
        // (massive-search.execution.default-lookback-months, attivo in prod) e' applicato dai soli
        // report via AnalysisWindowResolver: per un'istanza FILTER senza paymentPeriod il perimetro
        // puo' quindi contenere chiavi piu' vecchie del lookback, che non producono righe nei report.
        // La divergenza e' nulla finche' il lookback coincide con la retention dei dati online.
        if (period.getFrom() != null) {
            conditions.add("t.inserted_timestamp >= :paymentFrom");
            params.addValue("paymentFrom", period.getFrom());
        }
        if (period.getTo() != null) {
            // datetime al secondo: 'from' inclusivo, 'to' esclusivo (coerente con ReportWindowSql)
            conditions.add("t.inserted_timestamp < :paymentTo");
            params.addValue("paymentTo", period.getTo());
        }
    }

    private void appendPaymentStatuses(List<PerimeterPaymentStatus> statuses, List<String> conditions, MapSqlParameterSource params) {
        if (CollectionUtils.isEmpty(statuses)) {
            return;
        }
        List<String> outcomeValues = new ArrayList<>();
        boolean includeNoOutcome = false;
        for (PerimeterPaymentStatus status : statuses) {
            if (status == null) {
                continue;
            }
            switch (status) {
                case OK -> outcomeValues.add("OK");
                case KO -> outcomeValues.add("KO");
                case NO_OUTCOME -> includeNoOutcome = true;
            }
        }
        List<String> parts = new ArrayList<>();
        if (!outcomeValues.isEmpty()) {
            parts.add("t.outcome IN (:paymentOutcomes)");
            params.addValue("paymentOutcomes", outcomeValues);
        }
        if (includeNoOutcome) {
            parts.add("(t.outcome IS NULL OR t.outcome = '')");
        }
        if (!parts.isEmpty()) {
            conditions.add("(" + String.join(" OR ", parts) + ")");
        }
    }

    private void appendAmount(PerimeterFilter.AmountFilter amount, List<String> conditions, MapSqlParameterSource params) {
        if (amount == null) {
            return;
        }
        if (amount.getExact() != null) {
            conditions.add("t.amount = :amountExact");
            params.addValue("amountExact", amount.getExact());
            return;
        }
        if (amount.getMin() != null) {
            conditions.add("t.amount >= :amountMin");
            params.addValue("amountMin", amount.getMin());
        }
        if (amount.getMax() != null) {
            conditions.add("t.amount <= :amountMax");
            params.addValue("amountMax", amount.getMax());
        }
    }

    /**
     * I creditors arrivano dal BE come id di {@code anag_pa_emittente}, mentre {@code position.pa_emittente}
     * contiene il codice testuale: la risoluzione avviene con una subquery sull'anagrafica.
     */
    private void appendCreditors(List<Integer> creditors, List<String> conditions, MapSqlParameterSource params) {
        if (CollectionUtils.isEmpty(creditors)) {
            return;
        }
        conditions.add("p.pa_emittente IN (SELECT pae.codice FROM " + schema + ".anag_pa_emittente pae WHERE pae.id IN (:creditors))");
        params.addValue("creditors", creditors);
    }

    private void appendTechnologicalPartners(List<Integer> partners, List<String> conditions, MapSqlParameterSource params) {
        if (CollectionUtils.isEmpty(partners)) {
            return;
        }
        conditions.add("(t.intermediario_pa IN (:technologicalPartners) OR t.intermediario_psp IN (:technologicalPartners))");
        params.addValue("technologicalPartners", partners);
    }

    private void appendInStrings(String column, String paramName, List<String> values, List<String> conditions, MapSqlParameterSource params) {
        if (CollectionUtils.isEmpty(values)) {
            return;
        }
        conditions.add(column + " IN (:" + paramName + ")");
        params.addValue(paramName, values);
    }

    private void appendInIntegers(String column, String paramName, List<Integer> values, List<String> conditions, MapSqlParameterSource params) {
        if (CollectionUtils.isEmpty(values)) {
            return;
        }
        conditions.add(column + " IN (:" + paramName + ")");
        params.addValue(paramName, values);
    }
}
