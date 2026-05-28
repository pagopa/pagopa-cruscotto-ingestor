package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.entity.DataLayerExtraInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataLayerExtraInfoRepository extends JpaRepository<DataLayerExtraInfo, String> {
}
