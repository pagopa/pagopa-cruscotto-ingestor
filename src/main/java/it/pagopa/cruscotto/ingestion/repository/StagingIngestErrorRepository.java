package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StagingIngestErrorRepository extends JpaRepository<StagingIngestError, Long> {

    @Query(value = """
            SELECT *
            FROM ingestor.STG_INGEST_ERROR
            WHERE ENTITY_NAME = :entityName
              AND STATUS = :status
            ORDER BY CREATED_AT ASC
            LIMIT :limit
            """, nativeQuery = true)
    List<StagingIngestError> findPendingByEntity(
            @Param("entityName") String entityName,
            @Param("status") String status,
            @Param("limit") int limit
    );
}

