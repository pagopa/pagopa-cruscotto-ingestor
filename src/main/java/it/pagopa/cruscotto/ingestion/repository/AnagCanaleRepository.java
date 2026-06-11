package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagCanale;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagCanaleRepository extends JpaRepository<AnagCanale, Short> {
    @Query("SELECT a FROM AnagCanale a WHERE a.codice = :codice")
    Optional<AnagCanale> findByCodice(@Param("codice") String codice);
}
