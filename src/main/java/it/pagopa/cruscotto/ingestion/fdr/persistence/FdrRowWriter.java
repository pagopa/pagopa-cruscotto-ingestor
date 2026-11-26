package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;

/**
 * Opens a write session for one FDR file.
 *
 * <p>The stateful part of writing lives in {@link FdrWriteSession}, one per file, because the
 * counters, the truncation tally and the samples belong to a file and the service is a singleton
 * that two days could be running through at once. This class holds only what is shared and
 * immutable: the template, the transaction boundary, the schema and the batch size.
 *
 * <p><strong>Why a {@link TransactionTemplate} and not {@code @Transactional}.</strong> The
 * fallback needs a failed batch to be rolled back <em>before</em> the halves are attempted, and the
 * attempts are made from inside the same object. A {@code @Transactional} method called from its
 * own class is not proxied, so the annotation would be ignored and the "rollback" would never
 * happen — every sub-batch would run in the caller's transaction, already marked rollback-only.
 * The template makes the boundary real and visible at the call site.
 *
 * <p>{@code PROPAGATION_REQUIRES_NEW} and not {@code REQUIRED}: each batch must stand or fall on
 * its own. Under {@code REQUIRED} a batch failing inside a caller's transaction would poison it,
 * and the rows already written would be lost at the end — which is the opposite of isolating a bad
 * row.
 */
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrRowWriter {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final FdrPartitionGuard partitionGuard;
    private final String schema;
    private final int batchSize;

    public FdrRowWriter(JdbcTemplate jdbcTemplate,
                        PlatformTransactionManager transactionManager,
                        FdrPartitionGuard partitionGuard,
                        DbSchemaConfig dbSchemaConfig,
                        FdrProperties fdrProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(
                TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.partitionGuard = partitionGuard;
        this.schema = dbSchemaConfig.getSchemaName();
        this.batchSize = fdrProperties.getProcessing().getBatchSize();
    }

    /**
     * Opens the session for a file, after checking the day can be written at all.
     *
     * <p>The partition is checked here rather than on the first batch so that a day outside the
     * horizon fails before the file is downloaded and parsed, instead of after.
     *
     * @throws FdrPersistenceException with {@code MISSING_PARTITION} when the day would fall into
     *                                 the DEFAULT partition
     */
    public FdrWriteSession openSession(FdrBlobFile file, FdrDayFence fence) {
        return openSession(FdrWriteContext.of(Objects.requireNonNull(file, "file")), fence);
    }

    /**
     * @param fence il diritto di scrivere questa giornata, verificato dentro ogni transazione di
     *              batch. Non e' un parametro facoltativo mascherato: passare
     *              {@link FdrDayFence#NONE} su un percorso che scrive dato business riapre
     *              esattamente la finestra per cui la chiusura esiste — due worker che scrivono la
     *              stessa giornata e la raddoppiano in silenzio, perche' l'INSERT e' fedele e non
     *              deduplica. Lo hanno legittimamente solo i test del livello di scrittura
     * @see #openSession(FdrBlobFile, FdrDayFence)
     */
    public FdrWriteSession openSession(FdrWriteContext context, FdrDayFence fence) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(fence, "fence");
        partitionGuard.requireExplicitPartition(context.dataset(), context.businessDay());
        return new FdrWriteSession(context, jdbcTemplate, transactionTemplate, schema, batchSize,
                fence);
    }
}
