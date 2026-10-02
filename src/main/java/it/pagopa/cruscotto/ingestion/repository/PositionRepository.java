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

	Optional<Position> findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
			String nav,
			String paEmittente,
			LocalDateTime sourceInsertedTs
	);

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

	default Optional<Integer> findLatestIdByBusinessKeyBeforeTimestamp(String nav,
												String paEmittente,
												LocalDateTime sourceInsertedTs) {
		return findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
				nav,
				paEmittente,
				sourceInsertedTs
		).map(Position::getId);
	}

	/**
	 * Latest POSITION for (nav, paEmittente) within the 24h before {@code sourceInsertedTs}, with
	 * partition pruning.
	 *
	 * <p>POSITION is RANGE-partitioned monthly on DATE_EVENT with partitions pre-created years ahead,
	 * so a lookup constrained only by INSERTED_TIMESTAMP probes every partition (~25 index seeks per
	 * row). Deriving the DATE_EVENT window from the 24h span lets PostgreSQL scan only the 1-2
	 * partitions actually involved. The result is identical to the unpruned
	 * {@code ...InsertedTimestampLessThanEqual...} lookup followed by a {@code secondsDiff <= 86400}
	 * filter: the SQL {@code INSERTED_TIMESTAMP BETWEEN (ts - 24h) AND ts} enforces the same bound.
	 * Same DATE_EVENT⇔INSERTED_TIMESTAMP assumption already used by the batch prefetcher.</p>
	 */
	default Optional<Position> findLatestByBusinessKeyWithin24h(String nav,
												String paEmittente,
												LocalDateTime sourceInsertedTs) {
		LocalDateTime fromInclusive = sourceInsertedTs.minusHours(24);
		return findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
				nav,
				paEmittente,
				fromInclusive.toLocalDate(),
				sourceInsertedTs.toLocalDate(),
				fromInclusive,
				sourceInsertedTs
		);
	}
}
