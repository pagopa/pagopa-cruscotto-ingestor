package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PositionTransfersRepository extends JpaRepository<PositionTransfers, Integer> {
	Optional<PositionTransfers> findFirstByFkTokenAndPaTransferAndIdTransferOrderByIdDesc(
			Integer fkToken,
			String paTransfer,
			Short idTransfer
	);

	default Optional<PositionTransfers> findLatestByTokenAndTransferId(Integer fkToken, String paTransfer, Short idTransfer) {
		return findFirstByFkTokenAndPaTransferAndIdTransferOrderByIdDesc(fkToken, paTransfer, idTransfer);
	}
}
