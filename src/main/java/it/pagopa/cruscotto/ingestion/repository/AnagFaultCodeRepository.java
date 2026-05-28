package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagFaultCode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagFaultCodeRepository extends JpaRepository<AnagFaultCode, Short> {
    @Query("SELECT a FROM AnagFaultCode a WHERE a.codice = :codice")
    Optional<AnagFaultCode> findByCodice(@Param("codice") String codice);
}
