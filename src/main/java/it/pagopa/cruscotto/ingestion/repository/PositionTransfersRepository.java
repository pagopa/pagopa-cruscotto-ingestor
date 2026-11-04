package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

@Repository
public interface PositionTransfersRepository extends JpaRepository<PositionTransfers, Integer> {

	/** Transfer di un gruppo di token, usata dal riallineamento post-eventi. */
	List<PositionTransfers> findByFkTokenInOrderByFkTokenAscIdDesc(Collection<Integer> fkTokens);
}
