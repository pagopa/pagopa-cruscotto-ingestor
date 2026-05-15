package it.pagopa.cruscotto.ingestion.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzKustoImportJob extends QuartzJobBean {

    @Override
    protected void executeInternal(@NonNull JobExecutionContext context) {
        String runId = UUID.randomUUID().toString();
        String entityName = "KUSTO";

        log.info("START runId={} entityName={} phase=START", runId, entityName);
        try {
            // Legacy compatibility path: this class exists only to let old persisted Quartz rows deserialize.
            log.warn("NOOP runId={} entityName={} phase=NOOP message=Legacy KUSTO trigger is ignored", runId, entityName);
        } finally {
            log.info("END runId={} entityName={} phase=END", runId, entityName);
        }
    }
}

