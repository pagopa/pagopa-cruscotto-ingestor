package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PositionTokensRepository extends JpaRepository<PositionTokens, Long> {

    @Query(value = "SELECT pt.* FROM POSITION_TOKENS pt JOIN POSITIONS p ON pt.FK_POSITION = p.ID WHERE p.PA_EMITTENTE = :paEmittente AND p.NAV = :nav", nativeQuery = true)
    List<PositionTokens> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
