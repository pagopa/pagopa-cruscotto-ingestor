package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.DataLayerPositionTokens;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DataLayerPositionTokensRepository extends JpaRepository<DataLayerPositionTokens, String> {

    @Query(value = "SELECT * FROM DATALAYER_POSITION_TOKENS WHERE PA_EMITTENTE = :paEmittente AND NAV = :nav", nativeQuery = true)
    List<DataLayerPositionTokens> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
