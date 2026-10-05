package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
public interface StagingIngestErrorRepository extends JpaRepository<StagingIngestError, Long> {

    /**
     * Record PENDING di un'entita', in ordine di <strong>ultimo tentativo</strong>, limitati a quelli
     * nati dopo {@code createdAtFrom}.
     *
     * <p><strong>Perche' non per CREATED_AT.</strong> Ordinare per data di creazione produceva
     * <em>head-of-line blocking</em>: la query prende sempre la prima pagina, quindi i record piu'
     * vecchi venivano ripescati a ogni esecuzione. Se irrecuperabili (tipico dei figli il cui padre
     * non esiste), bloccavano la coda per giorni — venivano ritentati, fallivano, e un'ora dopo erano
     * di nuovo i piu' vecchi. Il throughput utile per i record effettivamente recuperabili era
     * prossimo a zero, indipendentemente dalla frequenza del job.</p>
     *
     * <p>{@code COALESCE(lastRetryAt, createdAt)} risolve la cosa senza stato aggiuntivo: chi non e'
     * mai stato tentato entra con la propria data di creazione, chi e' appena stato tentato va in
     * fondo. La coda diventa un round-robin equo, e un record avvelenato costa un tentativo per giro
     * invece di monopolizzare ogni esecuzione.</p>
     *
     * <p>Il bound su {@code createdAt} non e' un filtro funzionale ma il predicato che consente il
     * <strong>partition pruning</strong>: la tabella e' partizionata per giorno con 730 partizioni
     * pre-create, quindi una query senza vincolo le aprirebbe tutte. Coincide con la retention, oltre
     * la quale le partizioni vengono svuotate.</p>
     */
    @Query("SELECT s FROM StagingIngestError s"
            + " WHERE s.entityName = :entityName AND s.status = :status AND s.createdAt >= :createdAtFrom"
            + " ORDER BY COALESCE(s.lastRetryAt, s.createdAt) ASC")
    List<StagingIngestError> findPendingLeastRecentlyTried(
            @Param("entityName") String entityName,
            @Param("status") StagingStatus status,
            @Param("createdAtFrom") OffsetDateTime createdAtFrom,
            Pageable pageable
    );

    /** Arretrato corrente, per rendere visibile se la coda cresce o si smaltisce. */
    long countByStatusAndCreatedAtGreaterThanEqual(StagingStatus status, OffsetDateTime createdAtFrom);
}
