package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;


import java.util.List;

@Repository
public interface PositionTransfersRepository extends JpaRepository<PositionTransfers, String> {

    @Query(value = "SELECT * FROM POSITION_TRANSFERS WHERE PA_EMITTENTE = :paEmittente AND NAV = :nav", nativeQuery = true)
    List<PositionTransfers> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
