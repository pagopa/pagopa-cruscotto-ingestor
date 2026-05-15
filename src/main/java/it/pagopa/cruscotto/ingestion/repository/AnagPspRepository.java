package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagPsp;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagPspRepository extends JpaRepository<AnagPsp, Short> {
    @Query("SELECT a FROM AnagPsp a WHERE a.codice = :codice")
    Optional<AnagPsp> findByCodice(@Param("codice") String codice);
}
