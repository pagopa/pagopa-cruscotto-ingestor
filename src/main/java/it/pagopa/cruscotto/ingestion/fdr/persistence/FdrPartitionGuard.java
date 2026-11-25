package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobNameParser;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Locale;

/**
 * Refuses to write a day that has no partition of its own.
 *
 * <p>Both business tables have a DEFAULT partition, and that is exactly why this exists. Without
 * the DEFAULT, a day outside the 2026-2028 horizon would fail the INSERT and the problem would be
 * obvious; with it, the rows are accepted <strong>in silence</strong> and land in a partition that
 * cannot be pruned, grows without bound, slows every query on the table, and — the part that bites
 * later — is not the partition a reingestion truncates, so re-running that day would duplicate it
 * instead of replacing it.
 *
 * <p>The check is by partition name, and the name is ours: the Liquibase changeSet creates
 * {@code FDR_FLUSSI_RENDICONTAZIONE_20260915} with {@code format('%I', ...)}, hence quoted and
 * upper-case. Matching the convention rather than the partition bounds keeps the query simple and
 * version-independent; the trade is that the day FDR moves to monthly partitions — a declared
 * future option — this guard must move with it. That is not a hidden cost: a monthly layout also
 * changes what a reingestion truncates, so the two have to be decided together anyway.
 *
 * <p>Once per file, not per batch: it is a single catalogue lookup against a day that cannot
 * change while the file is being written.
 */
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrPartitionGuard {

    private static final String PARTITION_EXISTS_SQL =
            "SELECT count(*) FROM pg_catalog.pg_class c "
                    + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = ? AND c.relname = ? AND c.relispartition";

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public FdrPartitionGuard(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * @throws FdrPersistenceException with {@link FdrPersistenceException.Kind#MISSING_PARTITION}
     *                                 when the day would fall into the DEFAULT partition
     */
    public void requireExplicitPartition(FdrDataset dataset, LocalDate day) {
        String partition = partitionName(dataset, day);
        Long found = jdbcTemplate.queryForObject(PARTITION_EXISTS_SQL, Long.class,
                schema.toLowerCase(Locale.ROOT), partition);
        if (found == null || found == 0) {
            throw new FdrPersistenceException(FdrPersistenceException.Kind.MISSING_PARTITION,
                    "No explicit partition " + schema + "." + partition + " for FDR day " + day
                            + ": writing would silently fall into the DEFAULT partition");
        }
    }

    /** {@code FDR_FLUSSI_RENDICONTAZIONE} + {@code _20260915}, as the changeSet names them. */
    public static String partitionName(FdrDataset dataset, LocalDate day) {
        return tableName(dataset) + "_" + FdrBlobNameParser.DAY_FORMAT.format(day);
    }

    /** The business table of a dataset. The {@code FDR_} prefix is on the table, not on the file. */
    public static String tableName(FdrDataset dataset) {
        return dataset == FdrDataset.FLUSSI_RENDICONTAZIONE
                ? "FDR_FLUSSI_RENDICONTAZIONE"
                : "FDR_IUV_RENDICONTATI";
    }
}
