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
public class ExtraInfoIngestionRunner {
    private final GenericIngestionRunner genericIngestionRunner;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        RunContext context = new RunContext(EntityName.EXTRA_INFO.name(), runId, Instant.now());
        log.info("Starting ExtraInfoIngestionRunner runId={}", runId);
        genericIngestionRunner.runEntity(context);
        log.info("Completed ExtraInfoIngestionRunner runId={}", runId);
    }
}
