package it.pagopa.cruscotto.ingestion.fdr.persistence;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Set;

/**
 * Tells apart a database failure caused by one row from one caused by the database.
 *
 * <p>This is the decision the whole fallback hangs on. Splitting a batch is the right answer to a
 * value that does not fit a column, and the wrong answer to a closed connection: during an outage
 * a recursive split turns one failed batch into two thousand failed attempts, buries the real
 * cause and delays the retry that would actually help.
 *
 * <p><strong>The classification reads SQLSTATE, not the message.</strong> Messages are localised,
 * change between driver versions and are the first thing a vendor rewrites; SQLSTATE classes are
 * defined by the standard. Only two classes are treated as isolable:
 *
 * <ul>
 *   <li><strong>22 — data exception</strong>: value too long, invalid text representation, numeric
 *       out of range. These are properties of one value;</li>
 *   <li><strong>23 — integrity constraint violation</strong>: FDR's business tables carry no
 *       constraint but the primary key, so in practice this would be a duplicate {@code ID}, which
 *       is still a single row.</li>
 * </ul>
 *
 * <p>Everything else is technical and propagates: connection (08), insufficient resources (53,
 * which is the disk-full that already happened once in production), operator intervention (57),
 * transaction rollback (40), access rule violation (42, which includes a missing table or
 * partition), invalid authorisation (28). And so is <em>anything unrecognised</em>: the rule the
 * prompt and common sense agree on is that a cause which cannot be shown to be isolable must be
 * treated as technical, because the cost of being wrong in that direction is a delay, while the
 * other direction is a storm of retries against a database already in trouble.
 */
final class FdrSqlErrors {

    private static final Set<String> ISOLABLE_CLASSES = Set.of("22", "23");

    private FdrSqlErrors() {
    }

    /** The SQLSTATE of the first {@link SQLException} in the chain, or {@code null}. */
    static String sqlStateOf(Throwable error) {
        SQLException sql = sqlExceptionOf(error);
        return sql == null ? null : sql.getSQLState();
    }

    /**
     * The message worth keeping: the database's own, not the wrapper's.
     *
     * <p>Spring's translation layer replaces the message with a short description of what it
     * decided the failure was — "rejected", "could not execute statement" — while the driver's
     * says <em>"value too long for type character varying(30)"</em>, naming the column type and the
     * limit. The second is what turns a rejected row into a five-minute diagnosis, so the chain is
     * unwrapped to find it, and the wrapper is used only when there is no {@link SQLException}.
     */
    static String messageOf(Throwable error) {
        SQLException sql = sqlExceptionOf(error);
        if (sql != null && sql.getMessage() != null) {
            return sql.getMessage();
        }
        return error == null ? null : error.getMessage();
    }

    private static SQLException sqlExceptionOf(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof SQLException sql && sql.getSQLState() != null) {
                return sql;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return null;
    }

    /**
     * Whether the failure can be attributed to a single row, and therefore isolated by splitting.
     *
     * <p>A missing SQLSTATE answers {@code false}: no state means the failure did not come from
     * the server, which is the signature of a transport or driver problem.
     */
    static boolean isRowIsolable(Throwable error) {
        String sqlState = sqlStateOf(error);
        return sqlState != null && sqlState.length() >= 2
                && ISOLABLE_CLASSES.contains(sqlState.substring(0, 2));
    }

    static FdrPersistenceError.Category categoryOf(String sqlState) {
        if (sqlState == null || sqlState.length() < 2) {
            return FdrPersistenceError.Category.OTHER;
        }
        return switch (sqlState.substring(0, 2)) {
            case "22" -> FdrPersistenceError.Category.DATA_ERROR;
            case "23" -> FdrPersistenceError.Category.CONSTRAINT_VIOLATION;
            default -> FdrPersistenceError.Category.OTHER;
        };
    }

    /**
     * The message, on one line and bounded.
     *
     * <p>One line because a driver message with embedded newlines would break the single-line JSON
     * the logging standard requires; bounded because PostgreSQL happily quotes the offending value
     * back, and an error record is not the place to store a copy of the data that caused it.
     */
    static String shorten(String message, int maxLength) {
        if (message == null) {
            return null;
        }
        String flat = message.replace('\n', ' ').replace('\r', ' ').trim();
        return flat.length() <= maxLength ? flat : flat.substring(0, maxLength);
    }

    /** Lower-cased SQLSTATE class, for log keys. */
    static String classOf(String sqlState) {
        return sqlState == null || sqlState.length() < 2
                ? "unknown"
                : sqlState.substring(0, 2).toLowerCase(Locale.ROOT);
    }
}
