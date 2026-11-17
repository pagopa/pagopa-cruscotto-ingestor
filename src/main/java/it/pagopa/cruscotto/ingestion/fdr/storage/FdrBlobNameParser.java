package it.pagopa.cruscotto.ingestion.fdr.storage;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a blob path into either an {@link FdrBlobFile} or a classified {@link FdrInvalidEntry}.
 *
 * <p>Pure and static: no Azure, no configuration, no clock. Everything that can go wrong with an
 * FDR file name is decided here, which is why this class carries the tests rather than the service.
 *
 * <p><strong>It parses rather than matching one big regex</strong>, and the difference is
 * diagnostic, not stylistic. A single expression anchored on the expected day would answer "no" to
 * every malformed name alike, so a file dated the 16th in the folder of the 15th — a real symptom
 * of an upstream problem — would be indistinguishable from a stray document. Here the name is taken
 * apart one piece at a time and the first piece that fails names itself.
 *
 * <p>The order of the checks is itself a choice: the dataset comes first, so that a file which is
 * not ours at all is reported as {@code UNKNOWN_DATASET} without complaining about its extension.
 * Only once a file is recognised as FDR do the stricter checks apply to it.
 */
public final class FdrBlobNameParser {

    /** {@code yyyyMMdd}, used for both the day folder and the day inside the file name. */
    public static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    static final String EXTENSION = ".csv.gz";

    /**
     * {@code <8 digits>-<progressive>}. The progressive is captured loosely, as "everything after
     * the dash", and validated separately: matching it strictly here would turn {@code -X} and
     * {@code -0} into a generic malformed name instead of the precise reason they deserve.
     */
    private static final Pattern DAY_AND_PROGRESSIVE = Pattern.compile("^(\\d{8})-(.+)$");

    /**
     * Digits, and nothing else. Leading zeros are allowed on purpose.
     *
     * <p>The customer defined the naming but never defined the number of digits, the absence of
     * leading zeros, or an upper bound. Rejecting {@code -001} would be a rule of ours imposed on
     * their files, and it would do the one thing discovery must never do: lose a blob that exists.
     * {@code -1} and {@code -001} are two distinct blobs, both returned; whether having two
     * representations of the same number is an anomaly worth reporting is for the audit to decide,
     * not for the listing to decide by dropping one.
     *
     * <p>The value must still be strictly positive, which is what rules out {@code -0} and
     * {@code -000} — those are not a position in a sequence.
     */
    private static final Pattern PROGRESSIVE = Pattern.compile("^[0-9]+$");

    private FdrBlobNameParser() {
    }

    /** The day folder prefix for a date, trailing slash included: {@code 2026-09-15 -> 20260915/}. */
    public static String dayPrefix(LocalDate day) {
        return DAY_FORMAT.format(day) + '/';
    }

    /**
     * Classifies one blob path.
     *
     * @param blobName    full path inside the container, day folder included
     * @param expectedDay the day being discovered; the day inside the name must equal it
     */
    public static Outcome parse(String blobName, LocalDate expectedDay) {
        // Fail-fast con un messaggio, invece di una NullPointerException nuda dentro un ciclo su
        // migliaia di blob: un nome assente non e' un'anomalia del dato da classificare, e' un
        // difetto di chi chiama. Il servizio gia' scarta le entry senza nome prima di arrivare qui.
        Objects.requireNonNull(blobName, "blobName");
        String prefix = dayPrefix(expectedDay);
        String fileName = blobName.startsWith(prefix) ? blobName.substring(prefix.length()) : blobName;

        if (fileName.indexOf('/') >= 0) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.NESTED_PATH);
        }

        Optional<FdrDataset> dataset = FdrDataset.matching(fileName);
        if (dataset.isEmpty()) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.UNKNOWN_DATASET);
        }

        if (!fileName.endsWith(EXTENSION)) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.WRONG_EXTENSION);
        }

        String middle = fileName.substring(
                dataset.get().filePrefix().length() + 1,
                fileName.length() - EXTENSION.length());

        Matcher matcher = DAY_AND_PROGRESSIVE.matcher(middle);
        if (!matcher.matches()) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.MALFORMED_NAME);
        }

        LocalDate fileDay;
        try {
            fileDay = LocalDate.parse(matcher.group(1), DAY_FORMAT);
        } catch (DateTimeParseException e) {
            // Eight digits that are not a date, such as 20261345. Malformed, not "wrong day":
            // there is no day to compare.
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.MALFORMED_NAME);
        }
        if (!fileDay.equals(expectedDay)) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.WRONG_DAY);
        }

        String rawProgressive = matcher.group(2);
        if (!PROGRESSIVE.matcher(rawProgressive).matches()) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.INVALID_PROGRESSIVE);
        }
        // BigInteger and not int or long: the customer set no limit on the number of digits, so a
        // formally valid file name must never turn into a technical failure of the day because the
        // number did not fit. There is no parse to guard against either — the digits are already
        // verified above, so this cannot throw.
        BigInteger progressive = new BigInteger(rawProgressive);
        if (progressive.signum() <= 0) {
            return Outcome.invalid(blobName, FdrInvalidEntry.Reason.INVALID_PROGRESSIVE);
        }

        return Outcome.valid(blobName, dataset.get(), fileDay, progressive);
    }

    /**
     * The result of parsing: a dataset, day and progressive, or the reason none could be read.
     *
     * <p>Carries no Azure metadata — size, ETag and last-modified come from the listing and are
     * attached by the service. The split keeps this class free of anything that needs a network.
     */
    public record Outcome(
            String blobName,
            FdrDataset dataset,
            LocalDate businessDay,
            BigInteger progressive,
            FdrInvalidEntry.Reason invalidReason) {

        static Outcome valid(String blobName, FdrDataset dataset, LocalDate day, BigInteger progressive) {
            return new Outcome(blobName, dataset, day, progressive, null);
        }

        static Outcome invalid(String blobName, FdrInvalidEntry.Reason reason) {
            return new Outcome(blobName, null, null, null, reason);
        }

        public boolean isValid() {
            return invalidReason == null;
        }

        /** The classified anomaly. Only meaningful when {@link #isValid()} is {@code false}. */
        public FdrInvalidEntry toInvalidEntry() {
            return new FdrInvalidEntry(blobName, invalidReason);
        }
    }
}
