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
public class EventsWfIngestionRunner {
    private final GenericIngestionRunner genericIngestionRunner;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        RunContext context = new RunContext(EntityName.EVENTS_WF.name(), runId, Instant.now());
        log.info("jobTag=eventsWfJob Starting EventsWfIngestionRunner runId={}", runId);
        try {
            genericIngestionRunner.runEntity(context);
            log.info("jobTag=eventsWfJob Completed EventsWfIngestionRunner runId={}", runId);
        } catch (Throwable t) {
            log.error("jobTag=eventsWfJob Failed EventsWfIngestionRunner runId={} error={}", runId, t.getMessage(), t);
            throw t;
        }
    }
}
