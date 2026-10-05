package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
public interface StagingIngestErrorRepository extends JpaRepository<StagingIngestError, Long> {

    /**
     * Record PENDING di un'entita', dal piu' vecchio, limitati a quelli nati dopo {@code createdAtFrom}.
     *
     * <p>Il bound su {@code CREATED_AT} non e' un filtro funzionale ma il predicato che consente il
     * <strong>partition pruning</strong>: {@code STG_INGEST_ERROR} e' partizionata per giorno con 730
     * partizioni pre-create, quindi una query senza vincolo su {@code CREATED_AT} le aprirebbe tutte.
     * Il limite coincide con la retention, oltre la quale le partizioni vengono svuotate: i record
     * esclusi sono quelli che stanno per essere eliminati comunque, e ritentarli sarebbe banda
     * sottratta a quelli ancora recuperabili.</p>
     */
    List<StagingIngestError> findByEntityNameAndStatusAndCreatedAtGreaterThanEqualOrderByCreatedAtAsc(
            String entityName,
            StagingStatus status,
            OffsetDateTime createdAtFrom,
            Pageable pageable
    );
}
