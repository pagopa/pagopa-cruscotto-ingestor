package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrInstanceId;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Chi possiede una giornata FDR, per quanto, e come lo si dimostra scrivendo.
 *
 * <h2>Che problema risolve</h2>
 *
 * <p>Uno stato {@code PROCESSING} ha due significati opposti: un worker che sta lavorando, o un
 * worker morto a metà. Fino alla separazione dal vecchio ingestor la distinzione veniva da
 * {@code INGEST_EXECUTION_LOG} — "esiste un'esecuzione FDR che batte?" — e quella domanda è
 * <strong>troppo grossolana</strong>: dice che qualcuno sta lavorando, non che <em>questa</em>
 * giornata è di qualcuno. Due pod potevano quindi elaborare la stessa giornata e scrivere
 * entrambi, perché nulla legava la scrittura al diritto di eseguirla.
 *
 * <p>Qui il diritto è esplicito e ha una scadenza: un <strong>token</strong> opaco e un
 * <strong>lease</strong>. Chi vuole scrivere lo dimostra con un UPDATE condizionato al token, nella
 * stessa transazione della scrittura; chi non lo dimostra non scrive.
 *
 * <h2>L'unità è la giornata: due righe, un token solo</h2>
 *
 * <p>Ogni giornata è esattamente due righe di {@code FDR_INGESTION_STATUS}, una per dataset, e il
 * claim le prende insieme o non le prende. Un claim parziale sarebbe il caso peggiore possibile per
 * la reingestione, che svuota le due partizioni in un colpo solo: un worker padrone dei soli IUV
 * cancellerebbe anche i FLUSSI di un altro.
 *
 * <p>L'invariante "stesso token, stessa scadenza, stesso tentativo sulle due righe" non è
 * esprimibile come CHECK — è una condizione fra righe diverse — quindi è garantita qui: una sola
 * UPDATE che tocca entrambe e pretende {@code updated = 2}, dentro una transazione.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrDayOwnership {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final String schema;
    private final Duration leaseDuration;
    private final Duration heartbeatInterval;
    private final Clock clock;

    /**
     * Il costruttore di produzione.
     *
     * <p><strong>{@code @Autowired} qui non e' decorativo e non va tolto.</strong> La classe ha due
     * costruttori — questo e quello con il {@link Clock} usato dai test — e quando i costruttori sono
     * piu' di uno Spring non ne sceglie nessuno: ricade sul costruttore senza argomenti, che non
     * esiste, e il contesto non parte con
     * {@code BeanInstantiationException: No default constructor found}.
     */
    @Autowired
    public FdrDayOwnership(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
                           DbSchemaConfig dbSchemaConfig, FdrProperties fdrProperties) {
        this(jdbcTemplate, transactionManager, dbSchemaConfig, fdrProperties, Clock.systemUTC());
    }

    FdrDayOwnership(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager,
                    DbSchemaConfig dbSchemaConfig, FdrProperties fdrProperties, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        // REQUIRES_NEW: il claim deve committare per conto suo. Se partecipasse a una transazione
        // del chiamante, i lock delle due righe resterebbero presi per tutta la lavorazione della
        // giornata — ore, su un BOOTSTRAP — e nessun altro pod potrebbe nemmeno leggerne lo stato.
        this.transactionTemplate.setPropagationBehavior(
                TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.schema = dbSchemaConfig.getSchemaName();
        this.leaseDuration = fdrProperties.getRecovery().getLeaseDuration();
        this.heartbeatInterval = fdrProperties.getRecovery().getHeartbeatInterval();
        this.clock = clock;
    }

    Instant now() {
        return clock.instant();
    }

    // ─── Claim ───────────────────────────────────────────────────────────────────

    /**
     * Prende in carico una giornata, entrambe le righe, in una sola transazione.
     *
     * <p>La sequenza è: creare le righe mancanti, bloccarle in ordine deterministico, verificare che
     * siano una coppia valida e in uno stato ammesso, poi un solo UPDATE che le porta tutte e due in
     * {@code PROCESSING} con lo stesso token, la stessa scadenza e lo stesso tentativo. Se l'UPDATE
     * non tocca esattamente due righe la transazione viene annullata: <strong>non esiste un claim
     * parziale</strong>.
     *
     * <p>{@code ORDER BY DATASET} nel {@code SELECT ... FOR UPDATE} non è un vezzo: due worker che
     * bloccassero le stesse due righe in ordine opposto si incastrerebbero a vicenda, e PostgreSQL
     * risolverebbe la cosa uccidendone uno con un deadlock invece di farlo semplicemente aspettare.
     *
     * <p>Il tentativo è {@code corrente + 1}, e <strong>il corrente deve essere lo stesso su
     * entrambe le righe</strong>: se divergono il claim si rifiuta invece di riallinearle. Il
     * perché sta in {@link #nextAttempt}, e vale la pena leggerlo prima di "sistemare" la cosa con
     * un {@code MAX}.
     *
     * @param eligible gli stati che rendono un dataset <strong>azionabile</strong>, cioè da fare.
     *                 {@code PROCESSING} ha una regola in più: è azionabile solo se il lease è
     *                 scaduto, cioè se il proprietario precedente non dà più segni di vita.
     *                 Attenzione: <em>non</em> è richiesto che tutte le righe siano in questi
     *                 stati — basta che lo sia almeno una, e che le altre siano concluse. Il
     *                 perché, che è un difetto già pagato, sta in {@link #isDayClaimable}
     * @return il lease, oppure vuoto se la giornata non è claimabile — perché è in uno stato non
     *         ammesso, perché qualcun altro la possiede, o perché un altro worker è arrivato prima
     * @throws FdrInconsistentDayException se le due righe non concordano sul tentativo corrente.
     *                                     Non è un esito "non claimabile" fra gli altri: è
     *                                     un'incoerenza che richiede una decisione umana, e
     *                                     restituire un {@code Optional} vuoto la renderebbe
     *                                     indistinguibile da "ci sta lavorando un altro pod"
     */
    public Optional<FdrDayLease> claim(long batchId, LocalDate businessDay,
                                       Set<FdrProcessingStatus> eligible, Instant now) {
        ensureDayRows(batchId, businessDay, now);
        return Optional.ofNullable(transactionTemplate.execute(status ->
                claimInTransaction(batchId, businessDay, eligible, now)));
    }

    private FdrDayLease claimInTransaction(long batchId, LocalDate businessDay,
                                           Set<FdrProcessingStatus> eligible, Instant now) {
        List<LockedStatus> locked = lockDay(batchId, businessDay);
        if (!isValidPair(batchId, businessDay, locked)) {
            return null;
        }
        if (!isDayClaimable(locked, eligible, now)) {
            return null;
        }

        int attemptNo = nextAttempt(batchId, businessDay, locked);
        String token = UUID.randomUUID().toString();
        Instant leaseExpiresAt = now.plus(leaseDuration);

        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS "
                        + "SET STATUS = ?, ATTEMPT_COUNT = ?, PROCESSING_TOKEN = ?, "
                        + "LEASE_EXPIRES_AT = ?, PROCESSING_INSTANCE_ID = ?, STARTED_AT = ?, "
                        + "UPDATED_AT = ?, NEXT_ATTEMPT_AT = NULL "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ?",
                FdrProcessingStatus.PROCESSING.name(), attemptNo, token,
                Timestamp.from(leaseExpiresAt), FdrInstanceId.get(), Timestamp.from(now),
                Timestamp.from(now), batchId, java.sql.Date.valueOf(businessDay));

        if (updated != FdrDataset.values().length) {
            // Non può accadere: le righe sono bloccate da questa transazione e sono esattamente
            // due. Se accadesse significherebbe che qualcuno le sta cancellando sotto di noi, e
            // proseguire scriverebbe dato business su una giornata di cui non si sa più nulla.
            throw new IllegalStateException("Day claim touched " + updated
                    + " status rows instead of " + FdrDataset.values().length
                    + " for business day " + businessDay + " of batch " + batchId);
        }

        Map<FdrDataset, Long> statusIds = new EnumMap<>(FdrDataset.class);
        Map<FdrDataset, FdrProcessingStatus> previous = new EnumMap<>(FdrDataset.class);
        for (LockedStatus row : locked) {
            statusIds.put(row.dataset(), row.id());
            previous.put(row.dataset(), row.status());
        }
        logClaim(batchId, businessDay, token, attemptNo, previous);
        return new FdrDayLease(batchId, businessDay, token, attemptNo, statusIds, previous,
                leaseExpiresAt);
    }

    /**
     * Crea le righe della giornata se non ci sono ancora, in {@code PENDING} e a zero tentativi.
     *
     * <p>Serve al DAILY, che è l'unico percorso a incontrare una giornata mai vista: BOOTSTRAP e
     * REINGESTION le righe se le trovano già, scritte dall'espansione della richiesta o dal
     * backend. {@code ON CONFLICT DO NOTHING} si appoggia al vincolo UNIQUE
     * {@code (BATCH_ID, BUSINESS_DAY, DATASET)} e rende l'operazione innocua fra più pod: chi
     * perde non vede un errore, vede zero righe inserite.
     *
     * <p>Tentativi a <strong>zero</strong> e non a uno: il tentativo lo assegna il claim, e questo
     * è lo stato "mai tentata". È anche la ragione per cui il primo tentativo reale è 1 e mai 0.
     */
    private void ensureDayRows(long batchId, LocalDate businessDay, Instant now) {
        for (FdrDataset dataset : FdrDataset.values()) {
            jdbcTemplate.update(
                    "INSERT INTO " + schema + ".FDR_INGESTION_STATUS "
                            + "(ID, BATCH_ID, BUSINESS_DAY, DATASET, STATUS, ATTEMPT_COUNT, "
                            + "CREATED_AT, UPDATED_AT) "
                            + "VALUES (nextval('" + schema + ".SQ_FDR_INGESTION_STATUS'), "
                            + "?, ?, ?, ?, 0, ?, ?) "
                            + "ON CONFLICT (BATCH_ID, BUSINESS_DAY, DATASET) DO NOTHING",
                    batchId, java.sql.Date.valueOf(businessDay), dataset.name(),
                    FdrProcessingStatus.PENDING.name(), Timestamp.from(now), Timestamp.from(now));
        }
    }

    private List<LockedStatus> lockDay(long batchId, LocalDate businessDay) {
        return jdbcTemplate.query(
                "SELECT ID, DATASET, STATUS, ATTEMPT_COUNT, LEASE_EXPIRES_AT FROM " + schema
                        + ".FDR_INGESTION_STATUS WHERE BATCH_ID = ? AND BUSINESS_DAY = ? "
                        + "ORDER BY DATASET FOR UPDATE",
                (rs, rowNum) -> new LockedStatus(
                        rs.getLong("ID"),
                        FdrDataset.valueOf(rs.getString("DATASET")),
                        FdrProcessingStatus.valueOf(rs.getString("STATUS")),
                        rs.getInt("ATTEMPT_COUNT"),
                        rs.getTimestamp("LEASE_EXPIRES_AT") == null
                                ? null
                                : rs.getTimestamp("LEASE_EXPIRES_AT").toInstant()),
                batchId, java.sql.Date.valueOf(businessDay));
    }

    /**
     * Il tentativo successivo, che esiste solo se le due righe erano d'accordo su quello corrente.
     *
     * <p><strong>Nessun riallineamento automatico.</strong> La prima stesura usava
     * {@code max(attempt) + 1} e sembrava la scelta prudente — riallinea invece di conservare uno
     * scarto — ma è sbagliata per una ragione concreta: con {@code FLUSSI=1} e {@code IUV=4} porta
     * la giornata a <strong>5</strong>, cioè all'ultimo tentativo disponibile, e il giro successivo
     * la chiude come esaurita. L'anomalia sparirebbe dalla vista <em>consumando</em> i tentativi che
     * servivano a recuperare la giornata. Un {@code MIN} avrebbe il difetto opposto, e prendere il
     * valore dell'ultima riga letta renderebbe il risultato dipendente dall'ordine.
     *
     * <p>La divergenza non è uno stato da cui ripartire: i due dataset vengono tentati insieme,
     * sempre, da una sola {@code UPDATE}. Se divergono, qualcosa ha scritto quelle righe fuori dal
     * claim — e finché non si sa cosa, proseguire è indovinare.
     *
     * @throws FdrInconsistentDayException sollevata <em>prima</em> di generare il token e di
     *                                     toccare qualunque riga: l'eccezione esce dalla
     *                                     {@code TransactionTemplate}, che annulla la transazione,
     *                                     quindi gli status restano esattamente come erano
     */
    private static int nextAttempt(long batchId, LocalDate businessDay, List<LockedStatus> locked) {
        int first = locked.get(0).attemptCount();
        for (LockedStatus row : locked) {
            if (row.attemptCount() != first) {
                throw new FdrInconsistentDayException(batchId, businessDay, describeAttempts(locked));
            }
        }
        return first + 1;
    }

    private static String describeAttempts(List<LockedStatus> locked) {
        List<String> parts = new ArrayList<>();
        locked.forEach(row -> parts.add(row.dataset().name() + "=" + row.attemptCount()));
        return String.join(" ", parts);
    }

    /**
     * Una giornata valida è esattamente un dataset ciascuno, né più né meno.
     *
     * <p>Rifiutare invece di adattarsi è deliberato: una riga sola, un dataset ripetuto o uno
     * sconosciuto significano che chi ha scritto quella richiesta non ha capito il contratto, e
     * proseguire vorrebbe dire indovinare cosa intendesse. Una giornata a metà elaborata a metà è
     * peggio di una giornata non elaborata, perché <em>sembra</em> a posto.
     */
    private boolean isValidPair(long batchId, LocalDate businessDay, List<LockedStatus> locked) {
        Set<FdrDataset> datasets = new java.util.LinkedHashSet<>();
        locked.forEach(row -> datasets.add(row.dataset()));
        boolean valid = locked.size() == FdrDataset.values().length
                && datasets.size() == FdrDataset.values().length;
        if (!valid) {
            try (MdcScope ignored = MdcScope.phase("FDR_CLAIM")
                    .with(LogFields.EVENT_OUTCOME, "failure");
                 LogDetails details = LogDetails.open()
                         .with("batch_id", batchId)
                         .with("business_day", businessDay.toString())
                         .with("status_rows", locked.size())
                         .with("distinct_datasets", datasets.size())) {
                log.error("Refused to claim an FDR day that does not carry one status per dataset");
            }
        }
        return valid;
    }

    /**
     * Se la <strong>giornata</strong> si può prendere in carico: almeno un dataset da fare, e
     * nessun dataset di qualcun altro.
     *
     * <h3>Perché la regola è della giornata e non della riga</h3>
     *
     * <p>La prima stesura pretendeva che <em>ogni</em> riga fosse in uno stato eleggibile, e il
     * difetto che ne derivava era grave e silenzioso. I due dataset si elaborano <strong>in
     * sequenza</strong>, quindi FLUSSI può concludersi bene e IUV fallire subito dopo: la giornata
     * resta con {@code FLUSSI = COMPLETED} e {@code IUV = WAITING_RETRY}, che non è un'anomalia ma
     * l'esito previsto di un guasto a metà strada. Con {@code eligible = {WAITING_RETRY}} la riga
     * FLUSSI non era eleggibile, il claim falliva, e il poller ritrovava quella giornata ogni cinque
     * minuti fallendo allo stesso modo — <strong>senza lasciare traccia</strong>, perché un claim
     * non riuscito è l'esito normale di due pod in corsa e non si logga. La giornata era persa per
     * sempre, e lo stesso valeva per il recupero delle abbandonate, con {@code PROCESSING} al posto
     * di {@code WAITING_RETRY}.
     *
     * <p>{@code FdrDayScope.RESUME} esiste proprio per questo caso — riporta il dataset concluso
     * invece di rielaborarlo — ma non veniva mai raggiunto: il cancello che lo precede non si
     * apriva.
     *
     * <h3>Le due condizioni, e perché servono entrambe</h3>
     *
     * <ul>
     *   <li><strong>almeno un dataset azionabile</strong>. Senza, una selezione obsoleta — un altro
     *       pod ha chiuso la giornata fra la query del poller e il claim — farebbe reclamare una
     *       giornata finita <em>consumando un tentativo</em> per non fare nulla; ripetuto, la
     *       spingerebbe al tetto dei cinque rendendola irrecuperabile;</li>
     *   <li><strong>gli altri devono essere conclusi</strong>, nel senso di
     *       {@link FdrProcessingStatus#isSettled()}. Un dataset ancora vivo appartiene a un altro
     *       worker, e scavalcarlo è esattamente ciò che il possesso esiste per impedire.</li>
     * </ul>
     */
    private static boolean isDayClaimable(List<LockedStatus> locked,
                                          Set<FdrProcessingStatus> eligible, Instant now) {
        boolean anyActionable = false;
        for (LockedStatus row : locked) {
            if (isClaimable(row, eligible, now)) {
                anyActionable = true;
            } else if (!row.status().isSettled()) {
                return false;
            }
        }
        return anyActionable;
    }

    /**
     * Se si può partire da questo stato.
     *
     * <p>{@code PROCESSING} è il caso interessante: è ammesso solo quando il lease è scaduto, cioè
     * quando il proprietario precedente non lo rinnova più. Un lease <strong>nullo</strong> su una
     * riga {@code PROCESSING} conta come scaduto — non come "appena preso" — perché significa che
     * nessuno può dimostrarne il possesso, e il claim non lascia mai quella combinazione: stato e
     * lease vengono scritti dalla stessa UPDATE.
     */
    private static boolean isClaimable(LockedStatus row, Set<FdrProcessingStatus> eligible,
                                       Instant now) {
        if (!eligible.contains(row.status())) {
            return false;
        }
        if (row.status() != FdrProcessingStatus.PROCESSING) {
            return true;
        }
        return row.leaseExpiresAt() == null || !row.leaseExpiresAt().isAfter(now);
    }

    // ─── Rinnovo e chiusura ──────────────────────────────────────────────────────

    /**
     * Rinnova il lease, a condizione che la giornata sia ancora di chi lo chiede.
     *
     * <p>È il cuore della chiusura transazionale.
     *
     * <h3>La condizione è il TOKEN, e soltanto quello</h3>
     *
     * <p>La prima stesura richiedeva <em>anche</em> {@code STATUS = 'PROCESSING'}, per paura che un
     * worker rinnovasse una giornata chiusa nel frattempo da un altro percorso. Era un difetto, e
     * grave: i due dataset si elaborano <strong>in sequenza</strong>, quindi quando il secondo
     * scrive il primo è già al suo esito — {@code COMPLETED_WITH_WARNING}, per dire. Con la
     * condizione sullo stato l'UPDATE toccava una riga invece di due, e il secondo dataset moriva
     * con una perdita di lease inventata. <strong>Nessuna giornata poteva completare entrambi i
     * dataset.</strong>
     *
     * <p>La paura era infondata: il token <em>è</em> la prova del possesso. Se un altro worker
     * subentra, il claim sovrascrive il token e il nostro non corrisponde più; se un altro percorso
     * chiude la giornata — {@code settleDay} — il token viene azzerato lì. In entrambi i casi
     * l'UPDATE tocca zero righe, che è esattamente il segnale voluto. Lo stato non aggiungeva
     * sicurezza, toglieva correttezza.
     *
     * <p>{@code ATTEMPT_COUNT} <strong>non</strong> viene toccato: il rinnovo non è un nuovo
     * tentativo, e incrementarlo qui esaurirebbe i cinque disponibili in pochi minuti di lavoro
     * normale.
     *
     * <p>È uscito solo facendo girare l'ingestione contro PostgreSQL vero: con i mock,
     * {@code renew} risponde sempre di sì e il difetto non ha sintomi.
     *
     * <p>Nessuna transazione propria: deve partecipare a quella del chiamante, perché è da lì che
     * vengono i lock di riga che reggono la garanzia. Aprirne una nuova li rilascerebbe subito.
     *
     * @return {@code true} se ha toccato entrambe le righe. Qualsiasi altro numero significa
     *         possesso perso: una sola riga aggiornata è una giornata spezzata, e va trattata come
     *         perdita, mai come successo parziale
     */
    boolean renew(FdrDayLease lease, Instant now) {
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS "
                        + "SET LEASE_EXPIRES_AT = ?, UPDATED_AT = ? "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND PROCESSING_TOKEN = ?",
                Timestamp.from(now.plus(leaseDuration)), Timestamp.from(now),
                lease.batchId(), java.sql.Date.valueOf(lease.businessDay()), lease.token());

        if (updated != 0 && updated != FdrDataset.values().length) {
            try (MdcScope ignored = MdcScope.phase("FDR_LEASE")
                    .with(LogFields.EVENT_OUTCOME, "failure");
                 LogDetails details = LogDetails.open()
                         .with("batch_id", lease.batchId())
                         .with("business_day", lease.businessDay().toString())
                         .with("updated_rows", updated)) {
                log.error("FDR lease renewal touched a partial day: the two datasets are out of sync");
            }
        }
        return updated == FdrDataset.values().length;
    }

    /** La chiusura da passare al livello di scrittura e alle operazioni distruttive. */
    public FdrLeaseGuard guardFor(FdrDayLease lease) {
        return new FdrLeaseGuard(this, lease, heartbeatInterval, clock.instant());
    }

    // ─── Recupero delle giornate abbandonate ─────────────────────────────────────
    //
    // La QUERY che le trova non sta qui ma in FdrRetryService, accanto a quella delle giornate da
    // ritentare: sono due selezioni della stessa forma - giornata, batch, workflow - e il poller
    // le tratta con lo stesso codice. Tenerne una qui avrebbe significato una classe che espone un
    // metodo che nessuno chiama, che e' esattamente il difetto che questo progetto ha gia' pagato
    // una volta (vedi la trappola di metodo nell'handover di ingestion).
    //
    // Qui resta cio' che riguarda il possesso: dire se una RICHIESTA ha ancora qualcuno al lavoro.

    /**
     * Se <strong>qualcuna</strong> delle giornate di una richiesta è ancora posseduta da un worker.
     *
     * <p>Serve al recupero a livello di richiesta, che è l'unico posto in cui una soglia di tempo
     * resta necessaria: {@code FDR_INGESTION_BATCH} non ha un lease, perché l'unità di lavoro è la
     * giornata. La soglia da sola però dichiarerebbe morta una richiesta grande ancora viva, quindi
     * la decisione è "ferma da abbastanza <em>e</em> nessuno ci sta lavorando".
     *
     * <p>È la stessa domanda che prima si faceva a {@code INGEST_EXECUTION_LOG}, ma ristretta a
     * questa richiesta invece che a tutto FDR: prima bastava un qualunque pod FDR vivo nel cluster
     * — anche impegnato su un'altra richiesta — per bloccare il recupero di questa.
     */
    public boolean hasLiveLease(long batchId, Instant now) {
        Long live = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? AND STATUS = ? AND LEASE_EXPIRES_AT > ?",
                Long.class, batchId, FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now));
        return live != null && live > 0;
    }

    private static void logClaim(long batchId, LocalDate businessDay, String token, int attemptNo,
                                 Map<FdrDataset, FdrProcessingStatus> previous) {
        List<String> from = new ArrayList<>();
        previous.forEach((dataset, status) -> from.add(dataset.name() + "=" + status.name()));
        try (MdcScope ignored = MdcScope.phase("FDR_CLAIM");
             LogDetails details = LogDetails.open()
                     .with("batch_id", batchId)
                     .with("business_day", businessDay.toString())
                     .with("lease_token", token)
                     .with(LogFields.ATTEMPT, attemptNo)
                     .with("previous_status", String.join(",", from))) {
            log.info("Claimed an FDR business day");
        }
    }

    /** Lo stato di una riga nel momento in cui il claim la blocca. */
    private record LockedStatus(long id, FdrDataset dataset, FdrProcessingStatus status,
                                int attemptCount, Instant leaseExpiresAt) {
    }
}
