package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StagingIngestErrorRepository extends JpaRepository<StagingIngestError, Long> {
    List<StagingIngestError> findByEntityNameAndStatusOrderByCreatedAtAsc(
            String entityName,
            StagingStatus status,
            Pageable pageable
    );
}

