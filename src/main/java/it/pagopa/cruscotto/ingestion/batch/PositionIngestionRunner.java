package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class PositionIngestionRunner {
    private final GenericIngestionRunner genericIngestionRunner;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        RunContext context = new RunContext(EntityName.POSITION.name(), runId, Instant.now());
        log.info("jobTag=positionJob Starting PositionIngestionRunner runId={}", runId);
        try {
            genericIngestionRunner.runEntity(context);
            log.info("jobTag=positionJob Completed PositionIngestionRunner runId={}", runId);
        } catch (Throwable t) {
            log.error("jobTag=positionJob Failed PositionIngestionRunner runId={} error={}", runId, t.getMessage(), t);
            throw t;
        }
    }
}
