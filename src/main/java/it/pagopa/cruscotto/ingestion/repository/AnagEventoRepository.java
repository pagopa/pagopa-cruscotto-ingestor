package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.AnagEvento;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AnagEventoRepository extends JpaRepository<AnagEvento, Short> {
    @Query("SELECT a FROM AnagEvento a WHERE a.nomeEvento = :nomeEvento")
    Optional<AnagEvento> findByNomeEvento(@Param("nomeEvento") String nomeEvento);
}
