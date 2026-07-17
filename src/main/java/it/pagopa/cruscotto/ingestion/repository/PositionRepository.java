package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.Position;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
}
