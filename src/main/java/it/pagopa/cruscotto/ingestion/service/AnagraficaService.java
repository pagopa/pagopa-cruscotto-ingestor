package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service per la risoluzione e gestione delle anagrafiche ANAG_*.
 * <p>
 * Pattern di risoluzione (multi-pod safe):
 * 1. Cache in-memory con TTL (hit → return immediatamente).
 * 2. SELECT per trovare ID esistente.
 * 3. Se assente: INSERT INTO ANAG_* ON CONFLICT (codice) DO NOTHING.
 * 4. SELECT ID (garantito presente dopo l'upsert).
 * 5. Aggiornare cache.
 * <p>
 * Thread safety: ConcurrentHashMap + semantica DB deterministica.
 * Race condition multi-pod: gestite da UNIQUE constraint + ON CONFLICT DO NOTHING.
 */
@Slf4j
@Service
public class AnagraficaService {

    private static final String PHASE = "ANAG_LOOKUP";

    /** Width of the code/description columns of the ANAG_* tables ({@code VARCHAR(255)}). */
    private static final int MAX_ANAG_VALUE_LENGTH = 255;

    /** Upper bound on the distinct oversized values remembered for logging, to keep memory bounded. */
    private static final int MAX_TRUNCATION_WARN_ENTRIES = 1000;

    /** Oversized values already reported, so each dirty source value is logged once per pod. */
    private final Set<String> truncationWarned = ConcurrentHashMap.newKeySet();

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;
    private final Duration cacheTtl;
    private final boolean cacheEnabled;

    // Cache per tipo – key=valore logico, value=CacheEntry(id, expiresAtMs)
    private final ConcurrentHashMap<String, CacheEntry> stazioneCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> canaleCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> pspCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> paEmittenteCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> intermediarioPaCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> intermediarioPspCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> eventoCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheEntry> faultCodeCache = new ConcurrentHashMap<>();

    public AnagraficaService(
            NamedParameterJdbcTemplate jdbc,
            DbSchemaConfig dbSchemaConfig,
            IngestionConfig ingestionConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
        IngestionConfig.AnagraficaConfig.CacheConfig cacheConfig = ingestionConfig.getAnagrafica().getCache();
        this.cacheEnabled = cacheConfig.isEnabled();
        this.cacheTtl = Duration.ofMinutes(cacheConfig.getTtlMinutes());
    }

    // ---------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------

    /** Risolve o crea ANAG_STAZIONE per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveStazioneId(String runId, String codice) {
        return resolve(runId, "STAZIONE", codice, stazioneCache,
                table("ANAG_STAZIONE"), "CODICE", sequence("SQ_ANAG_STAZIONE"),
                Map.of("codice", codice));
    }

    /** Risolve o crea ANAG_CANALE per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveCanaleId(String runId, String codice) {
        return resolve(runId, "CANALE", codice, canaleCache,
                table("ANAG_CANALE"), "CODICE", sequence("SQ_ANAG_CANALE"),
                Map.of("codice", codice));
    }

    /** Risolve o crea ANAG_PSP per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolvePspId(String runId, String codice) {
        return resolve(runId, "PSP", codice, pspCache,
                table("ANAG_PSP"), "CODICE", sequence("SQ_ANAG_PSP"),
                Map.of("codice", codice));
    }

    /** Risolve o crea ANAG_PA_EMITTENTE per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolvePaEmittenteId(String runId, String codice) {
        return resolve(runId, "PA_EMITTENTE", codice, paEmittenteCache,
                table("ANAG_PA_EMITTENTE"), "CODICE", sequence("SQ_ANAG_PA_EMITTENTE"),
                Map.of("codice", codice));
    }

    /** Risolve o crea ANAG_INTERMEDIARIO_PA per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveIntermediarioPaId(String runId, String codice) {
        return resolve(runId, "INTERMEDIARIO_PA", codice, intermediarioPaCache,
                table("ANAG_INTERMEDIARIO_PA"), "CODICE", sequence("SQ_ANAG_INTERMEDIARIO_PA"),
                Map.of("codice", codice));
    }

    /** Risolve o crea ANAG_INTERMEDIARIO_PSP per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveIntermediarioPspId(String runId, String codice) {
        return resolve(runId, "INTERMEDIARIO_PSP", codice, intermediarioPspCache,
                table("ANAG_INTERMEDIARIO_PSP"), "CODICE", sequence("SQ_ANAG_INTERMEDIARIO_PSP"),
                Map.of("codice", codice));
    }

    /**
     * Risolve o crea ANAG_EVENTO per la coppia (tipoEvento, sottoTipoEvento).
     * Cache key = "tipo|sottoTipo".
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveEventoId(String runId, String tipoEvento, String sottoTipoEvento) {
        // Clamp BEFORE cache/SELECT/INSERT so the three always agree on the same key.
        String nomeEvento = clampToColumnWidth(runId, "EVENTO", tipoEvento);
        String normalizedSotto = clampToColumnWidth(runId, "EVENTO", sottoTipoEvento != null ? sottoTipoEvento : "");
        String cacheKey = buildEventoKey(nomeEvento, normalizedSotto);

        // Check cache
        if (cacheEnabled) {
            CacheEntry cached = eventoCache.get(cacheKey);
            if (cached != null && cached.isValid()) {
                return cached.id();
            }
        }

        // SELECT
        String selectSql = "SELECT ID FROM " + table("ANAG_EVENTO") +
                " WHERE NOME_EVENTO = :nome AND TIPO_EVENTO = :tipo";
        Long dbId = queryForId(selectSql, Map.of("nome", nomeEvento, "tipo", normalizedSotto));

        if (dbId == null) {
            // INSERT ON CONFLICT DO NOTHING
            String insertSql = "INSERT INTO " + table("ANAG_EVENTO") +
                    " (ID, NOME_EVENTO, TIPO_EVENTO)" +
                    " VALUES (nextval('" + sequence("SQ_ANAG_EVENTO") + "'), :nome, :tipo)" +
                    " ON CONFLICT (NOME_EVENTO, TIPO_EVENTO) DO NOTHING";
            jdbc.update(insertSql, new MapSqlParameterSource()
                    .addValue("nome", nomeEvento)
                    .addValue("tipo", normalizedSotto));

            dbId = queryForId(selectSql, Map.of("nome", nomeEvento, "tipo", normalizedSotto));
            if (dbId == null) {
                throw new IllegalStateException(
                        "[runId=" + runId + "] Cannot resolve EVENTO: tipo=" + nomeEvento + " sottoTipo=" + normalizedSotto);
            }
            log.info("[runId={}][phase={}] type=EVENTO value={} id={} source=insert", runId, PHASE, cacheKey, dbId);
        } else {
            log.debug("[runId={}][phase={}] type=EVENTO value={} id={} source=db", runId, PHASE, cacheKey, dbId);
        }

        if (cacheEnabled) eventoCache.put(cacheKey, newEntry(dbId));
        return dbId;
    }

    /** Risolve o crea ANAG_FAULT_CODE per il codice dato. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long resolveFaultCodeId(String runId, String codice) {
        return resolve(runId, "FAULT_CODE", codice, faultCodeCache,
                table("ANAG_FAULT_CODE"), "CODICE", sequence("SQ_ANAG_FAULT_CODE"),
                Map.of("codice", codice));
    }

    // ---------------------------------------------------------------
    // Backward-compat delegates (usati da EntityTransformerImpl)
    // ---------------------------------------------------------------

    /** @deprecated Usare {@link #resolveStazioneId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveStazione(String runId, String codice) {
        return toShort(resolveStazioneId(runId, codice));
    }

    /** @deprecated Usare {@link #resolveCanaleId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveCanale(String runId, String codice) {
        return toShort(resolveCanaleId(runId, codice));
    }

    /** @deprecated Usare {@link #resolvePspId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolvePsp(String runId, String codice) {
        return toShort(resolvePspId(runId, codice));
    }

    /** @deprecated Usare {@link #resolveIntermediarioPaId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveIntermediarioPa(String runId, String codice) {
        return toShort(resolveIntermediarioPaId(runId, codice));
    }

    /** @deprecated Usare {@link #resolveIntermediarioPspId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveIntermediarioPsp(String runId, String codice) {
        return toShort(resolveIntermediarioPspId(runId, codice));
    }

    /**
     * Risolve TIPO_EVENTO/SOTTO_TIPO_EVENTO singolo.
     * Usa sottoTipo vuoto per compatibilità.
     *
     * @deprecated Usare {@link #resolveEventoId(String, String, String)}
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveTipoEvento(String runId, String tipoEvento) {
        return toShort(resolveEventoId(runId, tipoEvento, ""));
    }

    /** @deprecated Usare {@link #resolveFaultCodeId(String, String)} */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Short resolveFaultCode(String runId, String codice) {
        return toShort(resolveFaultCodeId(runId, codice));
    }

    // ---------------------------------------------------------------
    // Core resolve logic (tabelle con singola colonna CODICE)
    // ---------------------------------------------------------------

    /**
     * Logica generica: cache → SELECT → INSERT ON CONFLICT → SELECT → cache.
     * Thread-safe: la races tra pod sono gestite da ON CONFLICT DO NOTHING + SELECT finale.
     */
    private long resolve(
            String runId,
            String anagType,
            String value,
            ConcurrentHashMap<String, CacheEntry> cache,
            String tableFqn,
            String column,
            String sequenceFqn,
            Map<String, Object> selectParams) {

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cannot resolve " + anagType + ": value is blank");
        }

        // Clamp BEFORE cache/SELECT/INSERT so the three always agree on the same key.
        String codice = clampToColumnWidth(runId, anagType, value);

        // 1. Cache hit (se abilitata)
        if (cacheEnabled) {
            CacheEntry cached = cache.get(codice);
            if (cached != null && cached.isValid()) {
                return cached.id();
            }
        }

        // 2. SELECT
        String selectSql = "SELECT ID FROM " + tableFqn + " WHERE " + column + " = :value";
        Long dbId = queryForId(selectSql, Map.of("value", codice));

        if (dbId != null) {
            if (cacheEnabled) cache.put(codice, newEntry(dbId));
            log.debug("[runId={}][phase={}] type={} value={} id={} source=db",
                    runId, PHASE, anagType, codice, dbId);
            return dbId;
        }

        // 3. INSERT ON CONFLICT DO NOTHING (multi-pod safe)
        String insertSql = "INSERT INTO " + tableFqn + " (ID, " + column + ")" +
                " VALUES (nextval('" + sequenceFqn + "'), :value)" +
                " ON CONFLICT (" + column + ") DO NOTHING";
        jdbc.update(insertSql, Map.of("value", codice));

        // 4. SELECT ID definitivo
        dbId = queryForId(selectSql, Map.of("value", codice));
        if (dbId == null) {
            throw new IllegalStateException(
                    "[runId=" + runId + "] Cannot resolve " + anagType + " for value=" + codice);
        }

        if (cacheEnabled) cache.put(codice, newEntry(dbId));
        log.info("[runId={}][phase={}] type={} value={} id={} source=insert",
                runId, PHASE, anagType, codice, dbId);
        return dbId;
    }

    /**
     * Clamps an anagrafica value to the width of the ANAG_* columns.
     *
     * <p>ADX can carry malformed values longer than {@code VARCHAR(255)}. Without this clamp the
     * INSERT fails with {@code value too long for type character varying(255)} and — since a SQL
     * failure raised during transformation is deliberately fail-fast — the whole ingestion run
     * aborts on that single row. The checkpoint is then never persisted, so the next run re-reads
     * the same row and fails again: one dirty value blocks the entity (and every child entity)
     * indefinitely. Truncation is deterministic, so cache, SELECT and INSERT resolve to the same
     * key and the value keeps mapping to a single anagrafica id.</p>
     */
    private String clampToColumnWidth(String runId, String anagType, String value) {
        if (value.length() <= MAX_ANAG_VALUE_LENGTH) {
            return value;
        }
        int end = MAX_ANAG_VALUE_LENGTH;
        // Never cut a UTF-16 surrogate pair in half: a lone surrogate would make PostgreSQL reject the
        // INSERT with "invalid byte sequence for encoding UTF8" — the very failure this clamp prevents.
        if (Character.isHighSurrogate(value.charAt(end - 1))) {
            end--;
        }
        String truncated = value.substring(0, end);
        // Log once per distinct oversized value: it is a source data-quality issue, not a run error.
        // Bounded so a flood of distinct dirty values cannot grow the set without limit.
        if (truncationWarned.size() < MAX_TRUNCATION_WARN_ENTRIES && truncationWarned.add(anagType + "|" + truncated)) {
            log.warn("[runId={}][phase={}] type={} oversized value truncated to {} chars"
                            + " (sourceLength={}), truncatedValue={}",
                    runId, PHASE, anagType, end, value.length(), truncated);
        }
        return truncated;
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private Long queryForId(String sql, Map<String, ?> params) {
        List<Long> results = jdbc.query(sql, new MapSqlParameterSource(params),
                (rs, rowNum) -> rs.getLong("ID"));
        return results.isEmpty() ? null : results.get(0);
    }

    private CacheEntry newEntry(long id) {
        return new CacheEntry(id, System.currentTimeMillis() + cacheTtl.toMillis());
    }

    private String table(String tableName) {
        return schema + "." + tableName;
    }

    private String sequence(String seqName) {
        return schema + "." + seqName;
    }

    private static String buildEventoKey(String tipo, String sottoTipo) {
        return tipo + "|" + (sottoTipo != null ? sottoTipo : "");
    }

    private static Short toShort(long id) {
        return (short) id;
    }

    // ---------------------------------------------------------------
    // Cache entry record (id + expiry)
    // ---------------------------------------------------------------

    private record CacheEntry(long id, long expiresAtMs) {
        boolean isValid() {
            return System.currentTimeMillis() < expiresAtMs;
        }
    }
}







