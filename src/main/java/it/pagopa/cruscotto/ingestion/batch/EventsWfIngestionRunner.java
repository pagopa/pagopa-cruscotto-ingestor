package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameters;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class EventsWfIngestionRunner {
    private final GenericIngestionRunner genericIngestionRunner;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        RunContext context = new RunContext(EntityName.EVENTS_WF.name(), runId, Instant.now());
        // Nessun log qui: questo runner e una delega sottile, e
        // GenericIngestionRunnerImpl emette gia START, il riepilogo di fine con i
        // contatori e l ERROR con lo stack. Le tre righe che stavano qui erano un
        // terzo strato sugli stessi dati, con l eccezione registrata tre volte.
        genericIngestionRunner.runEntity(context);
    }
}
