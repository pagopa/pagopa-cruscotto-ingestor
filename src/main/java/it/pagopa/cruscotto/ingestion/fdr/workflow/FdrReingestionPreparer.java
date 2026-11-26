package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileFormatException;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrDayFence;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPartitionGuard;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPersistenceException;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobContentReader;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobDiscoveryService;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDayDiscovery;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.InputStream;
import java.time.LocalDate;
import java.util.List;

/**
 * Prepara una giornata alla reingestione: verifica che sia ricostruibile, poi la svuota.
 *
 * <p>E' l'unico punto distruttivo di tutto FDR, ed e' costruito perche' <strong>non possa</strong>
 * svuotare qualcosa che non sarebbe poi in grado di riscrivere. Tre cancelli in fila, e il
 * {@code TRUNCATE} sta dopo l'ultimo:
 *
 * <ol>
 *   <li><strong>source preflight su entrambi i dataset.</strong> Se anche uno solo non e'
 *       ricostruibile — manca, e' vuoto, o e' illeggibile — non si tocca nulla. Mai una
 *       reingestione a meta': una giornata con i FLUSSI nuovi e gli IUV cancellati sarebbe peggio
 *       della giornata vecchia;</li>
 *   <li><strong>partizione esplicita</strong> per entrambe le tabelle, altrimenti il
 *       {@code TRUNCATE} non avrebbe nulla da svuotare e la riscrittura finirebbe nella DEFAULT;</li>
 *   <li><strong>DEFAULT vuota per quella giornata.</strong> Che la DEFAULT esista non basta: se
 *       contiene righe di D, svuotare la partizione esplicita le lascerebbe li', e la reingestione
 *       produrrebbe duplicati invece di una sostituzione.</li>
 * </ol>
 *
 * <p>Il preflight e' <strong>limitato</strong>: di ogni file legge al massimo il primo batch, il
 * necessario per dimostrare che contiene righe. Leggere per intero 400 MB per dirsi "si, ci sono
 * dati" raddoppierebbe il lavoro della giornata.
 *
 * <p><strong>Rischio residuo accettato (TOCTOU).</strong> Fra il preflight e l'elaborazione vera
 * passa del tempo, e in quel tempo i blob potrebbero cambiare: nel caso peggiore si tronca una
 * giornata sulla fede di file che poi non ci sono piu'. Non e' chiuso di proposito. Chiuderlo
 * significherebbe rileggere con accesso condizionale sull'ETag di ogni file e decidere cosa fare
 * di un cambiamento a meta' lavoro, cioe' una macchina a stati nuova; e la finestra si apre solo
 * se qualcuno riscrive una cartella giornaliera durante una reingestione, mentre la convenzione e'
 * che i file di una giornata sono pubblicati una volta. Il costo non e' nullo ma e' noto: se
 * succedesse, la giornata risulterebbe vuota o parziale e sarebbe recuperabile con una seconda
 * reingestione — nessun dato di <em>altre</em> giornate e' esposto.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrReingestionPreparer {

    private final FdrBlobDiscoveryService discoveryService;
    private final FdrBlobContentReader contentReader;
    private final FdrFileReader fileReader;
    private final FdrPartitionGuard partitionGuard;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final String schema;

    public FdrReingestionPreparer(FdrBlobDiscoveryService discoveryService,
                                  FdrBlobContentReader contentReader,
                                  FdrFileReader fileReader,
                                  FdrPartitionGuard partitionGuard,
                                  JdbcTemplate jdbcTemplate,
                                  PlatformTransactionManager transactionManager,
                                  DbSchemaConfig dbSchemaConfig) {
        this.discoveryService = discoveryService;
        this.contentReader = contentReader;
        this.fileReader = fileReader;
        this.partitionGuard = partitionGuard;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * Verifica che la giornata sia ricostruibile da entrambi i dataset.
     *
     * @return l'esito, con la ragione quando non lo e'
     * @throws RuntimeException i guasti tecnici (Azure, trasporto) salgono: non sono un "no", sono
     *                          un "non lo so", e il chiamante li classifichera' come ritentabili
     */
    public FdrPreflightResult preflight(LocalDate businessDay) {
        FdrDayDiscovery discovery = discoveryService.discover(businessDay);
        for (FdrDataset dataset : FdrDataset.values()) {
            List<FdrBlobFile> files = discovery.filesOf(dataset);
            if (files.isEmpty()) {
                return FdrPreflightResult.notRebuildable(dataset, "DATASET_MISSING",
                        "no file published for this day");
            }
            if (!hasAnyDataRow(files, dataset)) {
                return FdrPreflightResult.notRebuildable(dataset, "EMPTY_DATASET",
                        "no data row in any file of the dataset");
            }
        }
        return FdrPreflightResult.ok();
    }

    /**
     * Almeno una riga dati nel dataset, leggendo il meno possibile.
     *
     * <p>Un file con header e zero righe non squalifica il dataset: gli export sono prodotti a
     * intervalli fissi e una finestra senza traffico ne produce uno. Conta che <em>qualche</em>
     * file porti dati. Se invece sono vuoti tutti, la sorgente e' stata letta correttamente ed e'
     * vuota: rileggerla non cambierebbe nulla, e la giornata non e' ricostruibile.
     */
    private boolean hasAnyDataRow(List<FdrBlobFile> files, FdrDataset dataset) {
        for (FdrBlobFile file : files) {
            try (InputStream content = contentReader.open(file)) {
                fileReader.read(content, file, FdrRowMapper.forDataset(dataset),
                        FdrReingestionPreparer::stopAtFirstBatch);
            } catch (EnoughRowsException e) {
                return true;
            } catch (FdrFileFormatException e) {
                // Un file illeggibile non squalifica il dataset da solo - potrebbe essercene un
                // altro con i dati - ma non vale come prova di ricostruibilita'.
                try (MdcScope ignored = MdcScope.phase("FDR_PREFLIGHT");
                     LogDetails details = LogDetails.open()
                             .with(LogFields.EVENT_OUTCOME, "failure")
                             .with("business_day", file.businessDay().toString())
                             .with("dataset", dataset.name())
                             .with("blob_name", file.blobName())
                             .with(LogFields.REASON, e.kind().name())) {
                    log.warn("Skipped an unreadable file while checking the source");
                }
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Unable to read " + file.blobName(), e);
            }
        }
        return false;
    }

    /**
     * Ferma la lettura al primo batch.
     *
     * <p>Il lettore propaga le eccezioni dell'handler e chiude lo stream: e' il modo previsto per
     * interrompere un file, e qui lo si usa per non leggerlo tutto.
     */
    private static void stopAtFirstBatch(List<? extends FdrBusinessRow> batch) {
        if (!batch.isEmpty()) {
            throw new EnoughRowsException();
        }
    }

    /**
     * Svuota le due partizioni della giornata, insieme.
     *
     * <p>Un solo {@code TRUNCATE} per entrambe, dentro una sola transazione: o si svuotano tutte e
     * due o nessuna. Due statement con un commit in mezzo lascerebbero, se il secondo fallisse,
     * una giornata con meta' dei dati cancellati e l'altra meta' vecchia — lo stato peggiore
     * possibile, perche' sembra popolata.
     *
     * <p>La successiva ingestione sta <strong>fuori</strong> da questa transazione: tenerci dentro
     * milioni di righe significherebbe una transazione lunga quanto la giornata. Il prezzo e' una
     * finestra in cui la giornata e' parzialmente ricostruita, ed e' accettato: durante una
     * reingestione la giornata e' in lavorazione per definizione.
     *
     * <h2>La chiusura e' obbligatoria qui piu' che altrove</h2>
     *
     * <p>Questa e' l'unica operazione <strong>distruttiva</strong> del modulo, quindi e' quella in
     * cui scrivere senza averne il diritto fa il danno peggiore: un worker che ha perso il lease e
     * svuota la giornata cancellerebbe i dati che il suo successore sta ricostruendo in quel
     * momento, e il successore non se ne accorgerebbe — continuerebbe a inserire sopra il vuoto,
     * producendo una giornata incompleta che si dichiara completa.
     *
     * <p>La verifica sta quindi <em>dentro</em> la stessa transazione del {@code TRUNCATE}, prima
     * di esso: l'UPDATE condizionato al token prende i lock delle due righe di stato e li tiene
     * fino al commit. Una verifica fatta prima, fuori dalla transazione, lascerebbe aperta la
     * finestra fra il controllo e la cancellazione — che su un'operazione irreversibile e' la
     * differenza fra una garanzia e una speranza.
     *
     * @param fence il diritto di distruggere questa giornata
     * @throws FdrPersistenceException se una delle due guardie non passa
     * @throws FdrLeaseLostException se la giornata non e' piu' di questo worker: zero truncate
     */
    public void guardAndTruncate(LocalDate businessDay, FdrDayFence fence) {
        for (FdrDataset dataset : FdrDataset.values()) {
            partitionGuard.requireExplicitPartition(dataset, businessDay);
            requireEmptyDefaultPartition(dataset, businessDay);
        }

        String flussi = FdrPartitionGuard.partitionName(FdrDataset.FLUSSI_RENDICONTAZIONE, businessDay);
        String iuv = FdrPartitionGuard.partitionName(FdrDataset.IUV_RENDICONTATI, businessDay);
        // I nomi sono citati perche' le partizioni giornaliere sono maiuscole: senza virgolette
        // PostgreSQL le abbasserebbe e non le troverebbe. I valori non vengono da input esterno -
        // sono costruiti dal dataset e dalla giornata - quindi non c'e' nulla da parametrizzare.
        String sql = "TRUNCATE TABLE " + schema + ".\"" + flussi + "\", "
                + schema + ".\"" + iuv + "\"";
        transactionTemplate.executeWithoutResult(status -> {
            // Prima il diritto, poi la cancellazione, nella stessa transazione. Invertirli o
            // separarli in due transazioni riaprirebbe la finestra che questa forma chiude.
            fence.requireOwnership();
            jdbcTemplate.execute(sql);
        });

        // L'unica riga di log di tutto FDR che racconta una cancellazione. Porta i nomi delle due
        // partizioni perche' dopo il fatto sono l'unico modo per sapere cosa e' stato svuotato: le
        // righe non ci sono piu', e nessun'altra traccia le nomina.
        try (MdcScope ignored = MdcScope.phase("FDR_REINGESTION");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME, "success")
                     .with("business_day", businessDay.toString())
                     .with("flussi_partition", flussi)
                     .with("iuv_partition", iuv)) {
            log.info("Emptied both FDR partitions of the day for reingestion");
        }
    }

    /**
     * La DEFAULT non deve contenere la giornata.
     *
     * <p>La DEFAULT e' una rete di sicurezza: accoglie in silenzio le righe di una giornata senza
     * partizione. Se ne contiene per D, il {@code TRUNCATE} della partizione esplicita non le
     * toccherebbe e la reingestione le sommerebbe ai dati nuovi. Non vengono spostate in
     * automatico: significherebbe muovere dati di nascosto dentro un'operazione gia' distruttiva.
     */
    private void requireEmptyDefaultPartition(FdrDataset dataset, LocalDate businessDay) {
        String defaultPartition = FdrPartitionGuard.tableName(dataset).toLowerCase(java.util.Locale.ROOT)
                + "_default";
        Long rows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM " + schema + "." + defaultPartition + " WHERE DATE_INSERTED = ?",
                Long.class, java.sql.Date.valueOf(businessDay));
        if (rows != null && rows > 0) {
            throw new FdrPersistenceException(FdrPersistenceException.Kind.MISSING_PARTITION,
                    "The DEFAULT partition " + schema + "." + defaultPartition + " holds " + rows
                            + " rows for " + businessDay + ": a TRUNCATE of the explicit partition "
                            + "would leave them behind and the reingestion would duplicate the day");
        }
    }

    /** Segnale interno: il file ha righe, non serve leggere oltre. Senza stack trace. */
    private static final class EnoughRowsException extends RuntimeException {
        private EnoughRowsException() {
            super(null, null, false, false);
        }
    }

    /**
     * L'esito del preflight.
     *
     * @param dataset il dataset che ha fallito, {@code null} quando l'esito e' positivo
     */
    public record FdrPreflightResult(boolean rebuildable, FdrDataset dataset, String reasonCode,
                                     String reason) {

        static FdrPreflightResult ok() {
            return new FdrPreflightResult(true, null, null, null);
        }

        static FdrPreflightResult notRebuildable(FdrDataset dataset, String code, String reason) {
            return new FdrPreflightResult(false, dataset, code, reason);
        }
    }
}
