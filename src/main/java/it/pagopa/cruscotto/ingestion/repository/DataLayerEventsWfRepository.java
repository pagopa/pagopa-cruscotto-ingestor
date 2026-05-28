package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.DataLayerEventsWf;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataLayerEventsWfRepository extends JpaRepository<DataLayerEventsWf, String> {
}
