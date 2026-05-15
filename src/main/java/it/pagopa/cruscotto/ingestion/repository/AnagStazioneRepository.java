package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagStazione;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagStazioneRepository extends JpaRepository<AnagStazione, Short> {
    @Query("SELECT a FROM AnagStazione a WHERE a.codice = :codice")
    Optional<AnagStazione> findByCodice(@Param("codice") String codice);
}
