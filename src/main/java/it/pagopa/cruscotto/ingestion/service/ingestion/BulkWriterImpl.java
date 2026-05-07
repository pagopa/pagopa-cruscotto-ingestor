package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import it.pagopa.cruscotto.ingestion.repository.ExtraInfoRepository;
import it.pagopa.cruscotto.ingestion.repository.EventsWfRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class BulkWriterImpl implements BulkWriter {

    private final PositionRepository positionRepository;
    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;
    private final ExtraInfoRepository extraInfoRepository;
    private final EventsWfRepository eventsWfRepository;

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public BulkWriteResult writeBulk(EntityName entity, List<?> records) throws BulkWriteException {
        if (records == null || records.isEmpty()) {
            return new BulkWriteResult(0, Instant.now());
        }

        try {
            JpaRepository repository = getRepository(entity);

            // Bulk save using saveAll
            List<?> saved = repository.saveAll(records);

            log.debug("Bulk write completed for {} entity: {} records saved", entity, saved.size());

            // TODO: Find max timestamp from saved records if available
            Instant maxTimestamp = Instant.now();

            return new BulkWriteResult(saved.size(), maxTimestamp);
        } catch (Exception e) {
            String errorMsg = "Failed to bulk write " + records.size() + " records for entity " + entity;
            log.error(errorMsg, e);
            throw new BulkWriteException(errorMsg, e);
        }
    }

    @SuppressWarnings("rawtypes")
    private JpaRepository getRepository(EntityName entity) {
        return switch (entity) {
            case POSITION -> positionRepository;
            case POSITION_TOKENS -> positionTokensRepository;
            case POSITION_TRANSFERS -> positionTransfersRepository;
            case EXTRA_INFO -> extraInfoRepository;
            case EVENTS_WF -> eventsWfRepository;
            default -> throw new IllegalArgumentException("Bulk writer not configured for entity: " + entity);
        };
    }
}













