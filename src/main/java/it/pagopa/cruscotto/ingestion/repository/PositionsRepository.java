package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.Positions;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PositionsRepository extends JpaRepository<Positions, Long> {

    @Query(value = "SELECT * FROM POSITIONS WHERE PA_EMITTENTE = :paEmittente AND NAV = :nav", nativeQuery = true)
    List<Positions> findByPaEmittenteAndNav(@Param("paEmittente") String paEmittente, @Param("nav") String nav);

}
