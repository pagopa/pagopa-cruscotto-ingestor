package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagIntermediario;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagIntermediarioRepository extends JpaRepository<AnagIntermediario, Short> {
    @Query("SELECT a FROM AnagIntermediario a WHERE a.codice = :codice")
    Optional<AnagIntermediario> findByCodice(@Param("codice") String codice);
}
