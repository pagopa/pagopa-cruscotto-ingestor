package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.DataLayerPositions;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DataLayerPositionsRepository extends JpaRepository<DataLayerPositions, String> {

    @Query(value = "SELECT * FROM DATALAYER_POSITIONS WHERE PA_EMITTENTE = :paEmittente AND NAV = :nav", nativeQuery = true)
    List<DataLayerPositions> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
