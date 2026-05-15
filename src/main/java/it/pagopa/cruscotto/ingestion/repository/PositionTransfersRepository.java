package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PositionTransfersRepository extends JpaRepository<PositionTransfers, Integer> {
	@Query("SELECT pt FROM PositionTransfers pt WHERE pt.fkToken = ?1 AND pt.paTransfer = ?2 AND pt.idTransfer = ?3 " +
			"ORDER BY pt.id DESC LIMIT 1")
	Optional<PositionTransfers> findLatestByTokenAndTransferId(Integer fkToken, String paTransfer, Short idTransfer);
}
