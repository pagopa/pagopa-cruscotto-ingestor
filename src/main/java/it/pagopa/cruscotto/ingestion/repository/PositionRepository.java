package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.Position;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PositionRepository extends JpaRepository<Position, Integer> {
	Optional<Position> findFirstByNavAndPaEmittenteAndDateEventOrderByInsertedTimestampDescIdDesc(
			String nav,
			String paEmittente,
			LocalDate dateEvent
	);

	/**
	 * Latest POSITION for (nav, paEmittente) within the 24h before {@code toInclusive}: this is the
	 * lookup that decides whether a creation event merges into an existing position or starts a new
	 * one (rule 7.1). The caller derives the bounds, see {@code EntityTransformerImpl}.
	 *
	 * <p><strong>Why both DATE_EVENT and INSERTED_TIMESTAMP are bound.</strong> POSITION is
	 * RANGE-partitioned monthly on DATE_EVENT with partitions pre-created years ahead, so a lookup
	 * constrained only by INSERTED_TIMESTAMP probes every partition (~25 index seeks per row).
	 * Deriving the DATE_EVENT window from the same 24h span lets PostgreSQL scan only the 1-2
	 * partitions actually involved. The result is identical to the unpruned
	 * INSERTED_TIMESTAMP-only lookup: the timestamp predicate still enforces the real bound, and
	 * the date one only prunes. Same DATE_EVENT&hArr;INSERTED_TIMESTAMP assumption already used by
	 * the batch prefetcher.</p>
	 */
	Optional<Position> findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
			String nav,
			String paEmittente,
			LocalDate dateFrom,
			LocalDate dateTo,
			LocalDateTime fromInclusive,
			LocalDateTime toInclusive
	);

	/**
	 * POSITION rows of (nav, paEmittente) born in the given DATE_EVENT range, newest first.
	 *
	 * <p>Used by the DATE_EVENTS fallback of the child FK resolution: rule 7.1 merges a later event
	 * into an existing POSITION only within 24h of its birth, so a row that absorbed a given calendar
	 * day as an <em>additional</em> day was necessarily born on that same day or on the previous one.
	 * Restricting DATE_EVENT to those two days keeps the scan on 1-2 monthly partitions and lets the
	 * lookup use IDX_POSITION_NAV_PA, which is highly selective (~1 row per nav+pa).</p>
	 */
	List<Position> findByNavAndPaEmittenteAndDateEventBetweenOrderByInsertedTimestampDescIdDesc(
			String nav,
			String paEmittente,
			LocalDate dateFrom,
			LocalDate dateTo
	);

	default Optional<Integer> findLatestIdByBusinessKey(String nav, String paEmittente, LocalDate dateEvent) {
		return findFirstByNavAndPaEmittenteAndDateEventOrderByInsertedTimestampDescIdDesc(nav, paEmittente, dateEvent)
				.map(Position::getId);
	}

}
