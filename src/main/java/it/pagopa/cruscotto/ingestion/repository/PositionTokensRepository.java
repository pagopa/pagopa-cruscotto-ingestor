package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PositionTokensRepository extends JpaRepository<PositionTokens, String> {

    @Query(value = "SELECT * FROM POSITION_TOKENS WHERE PA_EMITTENTE = :paEmittente AND NAV = :nav", nativeQuery = true)
    List<PositionTokens> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
