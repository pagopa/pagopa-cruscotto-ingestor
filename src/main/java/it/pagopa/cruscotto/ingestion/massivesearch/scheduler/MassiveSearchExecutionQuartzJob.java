package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.UUID;

/**
 * Quartz scanner that periodically triggers pending Massive Search instances.
 *
 * <p>Pure scanner: it only delegates to {@link MassiveSearchExecutionSchedulerService} and returns
 * quickly. It contains no business logic, generates no report, reads no CSV and never queries
 * POSITION / TOKEN / TRANSFER. {@link DisallowConcurrentExecution} guarantees a single Massive Search
 * scan at a time (matching the ADX ingestion jobs' concurrency policy).</p>
 */
@Slf4j
@DisallowConcurrentExecution
public class MassiveSearchExecutionQuartzJob extends QuartzJobBean {

    private static final String ENTITY_NAME = "MASSIVE_SEARCH_SCANNER";

    @Autowired
    private MassiveSearchExecutionSchedulerService schedulerService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.EVENT_ACTION, ENTITY_NAME)
                .with(LogFields.CORRELATION_ID, runId)) {
            try (MdcScope phase = MdcScope.phase("START")) {
                log.info("Started job");
            }
            try {
                int submitted = schedulerService.scan(runId);
                try (MdcScope phase = MdcScope.phase("CHECKPOINT");
                     LogDetails details = LogDetails.open().with("submitted", submitted)) {
                    log.info("Submitted pending search executions");
                }
            } catch (Throwable t) {
                try (MdcScope phase = MdcScope.phase("ERROR");
                     LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, t.getMessage())) {
                    log.error("Job failed", t);
                }
                throw new JobExecutionException(t);
            } finally {
                // Nessun event_outcome: gira anche dopo un fallimento.
                try (MdcScope phase = MdcScope.phase("END")) {
                    log.info("Finished job");
                }
            }
        }
    }
}
