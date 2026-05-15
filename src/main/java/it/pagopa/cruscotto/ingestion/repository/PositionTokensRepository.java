package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

@Repository
public interface PositionTokensRepository extends JpaRepository<PositionTokens, Integer> {
	Optional<PositionTokens> findFirstByFkPositionAndIuvAndDateEventOrderByIdDesc(
			Integer fkPosition,
			String iuv,
			LocalDate dateEvent
	);

	Optional<PositionTokens> findFirstByTokenAndDateEventOrderByIdDesc(
			byte[] token,
			LocalDate dateEvent
	);

	@Query("SELECT pt FROM PositionTokens pt WHERE pt.token = ?1 ORDER BY pt.id DESC LIMIT 1")
	Optional<PositionTokens> findLatestByToken(byte[] token);

	default Optional<Integer> findLatestIdByPositionAndIuv(Integer fkPosition, String iuv, LocalDate dateEvent) {
		return findFirstByFkPositionAndIuvAndDateEventOrderByIdDesc(fkPosition, iuv, dateEvent)
				.map(PositionTokens::getId);
	}

	default Optional<Integer> findLatestIdByTokenAndDate(byte[] token, LocalDate dateEvent) {
		return findFirstByTokenAndDateEventOrderByIdDesc(token, dateEvent)
				.map(PositionTokens::getId);
	}
}
