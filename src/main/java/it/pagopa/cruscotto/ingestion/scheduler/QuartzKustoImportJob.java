package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
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

    /**
     * Percorso di sola compatibilita': questa classe esiste solo perche' le vecchie righe Quartz
     * persistite si possano deserializzare.
     *
     * <p>Emette <strong>una</strong> riga e non tre. Le coppie START/END hanno senso dove c'e' un
     * lavoro in mezzo di cui sapere se e' partito; qui in mezzo non c'e' nulla, quindi START ed END
     * attorno a un no-op erano due righe che non dicevano niente che la terza non dicesse meglio.
     */
    @Override
    protected void executeInternal(@NonNull JobExecutionContext context) {
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.CORRELATION_ID, UUID.randomUUID().toString())
                .with(LogFields.EVENT_ACTION, "KUSTO")
                .with(LogFields.CTX_PHASE, "NOOP")) {
            log.warn("Ignored legacy KUSTO trigger");
        }
    }
}
