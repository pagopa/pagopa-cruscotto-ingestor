package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PositionTransfersRepository extends JpaRepository<PositionTransfers, Long> {

    @Query(value = "SELECT ptr.* FROM POSITION_TRANSFERS ptr JOIN POSITION_TOKENS pt ON ptr.FK_TOKEN = pt.ID JOIN POSITIONS p ON pt.FK_POSITION = p.ID WHERE p.PA_EMITTENTE = :paEmittente AND p.NAV = :nav", nativeQuery = true)
    List<PositionTransfers> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
