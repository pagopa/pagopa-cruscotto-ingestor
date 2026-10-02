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
 * pairs over the existing SERT tables.
 *
 * <p><strong>Posizioni senza tentativi.</strong> Il report Position seleziona le posizioni per
 * data della posizione e deve riportare anche quelle senza alcun tentativo, coi campi del token
 * vuoti. Un perimetro calcolato solo con {@code position JOIN position_tokens} non le conterrebbe
 * mai, e il report Position non potrebbe quindi produrle per le ricerche a filtri. Per questo,
 * quando nessun filtro insiste sul tentativo, il perimetro unisce due rami:</p>
 *
 * <ul>
 *   <li>tentativi nel periodo (data del token): serve ai report Tentativi e Transfer, che
 *   selezionano per data del token;</li>
 *   <li>posizioni nel periodo (data della posizione): serve al report Position, e include quelle
 *   senza tentativi.</li>
 * </ul>
 *
 * <p>Nessuno dei due contiene l'altro. L'ingestion associa un token solo a una posizione delle 24
 * ore precedenti, quindi a cavallo di un bound del periodo posizione e token possono cadere da lati
 * opposti: una posizione del 31 gennaio alle 23:30 con il tentativo il 1 febbraio alle 00:10 sta
 * nel ramo posizioni per gennaio e nel ramo tentativi per febbraio.</p>
 *
 * <p>Con almeno un filtro sul tentativo (esito, touchpoint, importo, PSP, canale, ...) il ramo
 * posizioni non si applica: una posizione senza tentativi non puo' soddisfarlo.</p>
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

        // Condizioni sulla sola posizione: valgono per entrambi i rami.
        List<String> keyConditions = new ArrayList<>();
        keyConditions.add("p.nav IS NOT NULL");
        keyConditions.add("p.pa_emittente IS NOT NULL");
        appendCreditors(filter.getCreditors(), keyConditions, params);

        // Condizioni che esistono solo su un tentativo.
        List<String> tokenConditions = new ArrayList<>();
        appendPaymentStatuses(filter.getPaymentStatuses(), tokenConditions, params);
        appendInStrings("t.touchpoint", "touchpoints", filter.getTouchpoints(), tokenConditions, params);
        appendInStrings("t.payment_method", "paymentMethods", filter.getPaymentMethods(), tokenConditions, params);
        appendAmount(filter.getAmount(), tokenConditions, params);
        appendInIntegers("t.psp", "psps", filter.getPsps(), tokenConditions, params);
        appendInIntegers("t.intermediario_pa", "technologicalPartnersPa", filter.getTechnologicalPartnersPa(), tokenConditions, params);
        appendInIntegers("t.intermediario_psp", "technologicalPartnersPsp", filter.getTechnologicalPartnersPsp(), tokenConditions, params);
        appendInIntegers("t.canale", "channels", filter.getChannels(), tokenConditions, params);
        appendInIntegers("t.stazione", "stations", filter.getStations(), tokenConditions, params);

        PerimeterFilter.PaymentPeriod period = filter.getPaymentPeriod();
        bindPaymentPeriod(period, params);
        List<String> tokenPeriod = periodConditions("t", period);
        List<String> positionPeriod = periodConditions("p", period);

        String tokenBranch = "SELECT p.pa_emittente AS pa, p.nav AS nav"
            + " FROM " + schema + ".position p"
            + " JOIN " + schema + ".position_tokens t ON t.fk_position = p.id"
            + " WHERE " + String.join(" AND ", concat(keyConditions, tokenPeriod, tokenConditions));
        String positionBranch = "SELECT p.pa_emittente AS pa, p.nav AS nav"
            + " FROM " + schema + ".position p"
            + " WHERE " + String.join(" AND ", concat(keyConditions, positionPeriod));

        // Tre forme, decise dai filtri presenti:
        // - TOKEN: c'e' almeno un filtro sul tentativo. Una posizione senza tentativi non puo'
        //   soddisfarlo, quindi il ramo sulle posizioni non contribuirebbe nulla.
        // - POSITION: nessun filtro sul tentativo e nessun periodo. Ogni chiave del ramo TOKEN e'
        //   anche una posizione, quindi il JOIN sui tentativi sarebbe solo costo.
        // - UNION: nessun filtro sul tentativo ma un periodo. I due rami leggono il periodo su date
        //   diverse e nessuno dei due contiene l'altro (vedi javadoc di classe).
        String shape;
        String sql;
        if (!tokenConditions.isEmpty()) {
            shape = "TOKEN";
            sql = tokenBranch.replaceFirst("^SELECT ", "SELECT DISTINCT ");
        } else if (positionPeriod.isEmpty()) {
            shape = "POSITION";
            sql = positionBranch.replaceFirst("^SELECT ", "SELECT DISTINCT ");
        } else {
            shape = "UNION";
            // UNION, non UNION ALL: la deduplica delle chiavi e' il punto.
            sql = tokenBranch + " UNION " + positionBranch;
        }
        sql += " ORDER BY pa, nav";

        log.debug("phase=PERIMETER_QUERY shape={} keyConditions={} tokenConditions={} period={}",
            shape, keyConditions.size(), tokenConditions.size(), !positionPeriod.isEmpty());
        return new PerimeterQuery(sql, params);
    }

    @SafeVarargs
    private static List<String> concat(List<String>... parts) {
        List<String> all = new ArrayList<>();
        for (List<String> part : parts) {
            all.addAll(part);
        }
        return all;
    }

    private static void bindPaymentPeriod(PerimeterFilter.PaymentPeriod period, MapSqlParameterSource params) {
        if (period == null) {
            return;
        }
        // Date assolute: bind di LocalDateTime, nessuna conversione tz.
        if (period.getFrom() != null) {
            params.addValue("paymentFrom", period.getFrom());
        }
        if (period.getTo() != null) {
            params.addValue("paymentTo", period.getTo());
        }
    }

    /**
     * Predicati del periodo sull'alias indicato: {@code t} per i tentativi, {@code p} per le posizioni.
     *
     * <p>Finestra su {@code inserted_timestamp} (sorgente ADX, sempre valorizzato) e non su
     * {@code payment_date}, che e' null per i token non pagati: stessa colonna e stessi bound di
     * {@code ReportWindowSql}. Accanto, il bound su {@code date_event}: implicato dal primo
     * ({@code date_event = date(inserted_timestamp)}) e mai piu' restrittivo, serve solo al planner
     * per potare le ~25 partizioni mensili.</p>
     *
     * <p>NOTA: qui vengono applicati solo i bound indicati dall'utente. Il bound inferiore di default
     * ({@code massive-search.execution.default-lookback-months}, attivo in prod) e' applicato dai
     * soli report via {@code AnalysisWindowResolver}: per un'istanza FILTER senza paymentPeriod il
     * perimetro puo' quindi contenere chiavi piu' vecchie del lookback, che non producono righe nei
     * report. La divergenza e' nulla finche' il lookback coincide con la retention dei dati online.</p>
     */
    private static List<String> periodConditions(String alias, PerimeterFilter.PaymentPeriod period) {
        List<String> conditions = new ArrayList<>();
        if (period == null) {
            return conditions;
        }
        if (period.getFrom() != null) {
            conditions.add(alias + ".inserted_timestamp >= :paymentFrom");
            conditions.add(alias + ".date_event >= CAST(:paymentFrom AS date)");
        }
        if (period.getTo() != null) {
            // datetime al secondo: 'from' inclusivo, 'to' esclusivo (coerente con ReportWindowSql)
            conditions.add(alias + ".inserted_timestamp < :paymentTo");
            // '<=' e non '<': paymentTo e' esclusivo sul timestamp ma la sua troncatura a date e'
            // l'ultimo giorno ammissibile, che va incluso.
            conditions.add(alias + ".date_event <= CAST(:paymentTo AS date)");
        }
        return conditions;
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
