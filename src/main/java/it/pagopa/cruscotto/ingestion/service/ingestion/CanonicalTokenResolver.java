package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.repository.PositionTokenRegistryReader;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Single place where the canonical {@code POSITION_TOKENS} row is resolved from a TOKEN, with
 * partition pruning.
 *
 * <p>{@code POSITION_TOKENS} is RANGE-partitioned monthly on {@code DATE_EVENT}, and partitions are
 * pre-created for years ahead. A lookup by TOKEN alone cannot be pruned, so PostgreSQL probes the
 * index of <em>every</em> partition: with ~25 partitions that is ~25 index seeks per row instead of
 * one, which is what made the per-row FK fallback the dominant cost of the EVENTS_WF runs
 * (hundreds of thousands of lookups per run).</p>
 *
 * <p>{@code POSITION_TOKEN_REGISTRY} (not partitioned, PK on TOKEN) holds the canonical
 * {@code FIRST_DATE_EVENT}, i.e. the partition to hit: reading it first turns the scan of all
 * partitions into one PK seek plus one pruned seek. The unpruned lookup is kept only as a
 * last resort for tokens missing from the registry (legacy rows, or a purged registry), so
 * correctness is unchanged.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CanonicalTokenResolver {

    private final PositionTokensRepository positionTokensRepository;
    private final PositionTokenRegistryReader positionTokenRegistryReader;

    /** Resolves the canonical row for the token, pruning by partition when possible. */
    public Optional<PositionTokens> findCanonical(byte[] token) {
        if (token == null) {
            return Optional.empty();
        }
        Optional<LocalDate> firstDateEvent = positionTokenRegistryReader.findFirstDateEventByToken(token);
        if (firstDateEvent.isPresent()) {
            LocalDate prunedDate = firstDateEvent.orElseThrow(
                    () -> new IllegalStateException("FIRST_DATE_EVENT unexpectedly absent after presence check"));
            Optional<PositionTokens> byDate = positionTokensRepository.findCanonicalByTokenAndDate(token, prunedDate);
            if (byDate.isPresent()) {
                return byDate;
            }
        }
        // Registry miss: fall back to the unpruned lookup rather than losing the FK.
        return positionTokensRepository.findCanonicalByToken(token);
    }

    /** Convenience for callers that only need the FK. */
    public Optional<Integer> findCanonicalId(byte[] token) {
        return findCanonical(token).map(PositionTokens::getId);
    }
}
