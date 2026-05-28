package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.ExecutionLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;

@Repository
public interface ExecutionLogRepository extends JpaRepository<ExecutionLog, Long> {

    Optional<ExecutionLog> findFirstByRunIdAndEntityName(String runId, String entityName);

    long countByCreatedAtBefore(OffsetDateTime threshold);

    long deleteByCreatedAtBefore(OffsetDateTime threshold);
}

