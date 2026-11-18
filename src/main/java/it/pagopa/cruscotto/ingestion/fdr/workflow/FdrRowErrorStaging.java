package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowRejection;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPersistenceError;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registra in {@code STG_INGEST_ERROR} le righe che non sono arrivate in banca dati.
 *
 * <p><strong>Solo {@code insertDiscardedBulk}, mai {@code insertBulk}.</strong> Il secondo scrive
 * righe in stato {@code PENDING}, destinate alla riconciliazione ADX: una riga FDR messa lì non
 * verrebbe riconciliata da nessuno e resterebbe PENDING per sempre, inquinando una coda che
 * qualcun altro sorveglia. Le righe FDR non tornano: sono scarti terminali, e il percorso degli
 * scarti le scrive già {@code DONE} con la loro ragione.
 *
 * <p><strong>Quante ne arrivano.</strong> Solo il campione che lettore e scrittore hanno
 * conservato — venti per file ciascuno — non tutte. È una conseguenza voluta del limite di memoria
 * di quei due: su una giornata in cui il tracciato cambiasse, le righe scartate sarebbero
 * centinaia di migliaia, e copiarle qui significherebbe scrivere un secondo database per
 * descrivere il primo. Il <em>conteggio</em> completo sta in {@code FDR_INGESTION_STATUS.ROWS_FAILED},
 * che è il numero su cui si decide; il campione serve a capire di che forma fossero.
 */
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrRowErrorStaging {

    private final StagingErrorService stagingErrorService;

    public FdrRowErrorStaging(StagingErrorService stagingErrorService) {
        this.stagingErrorService = stagingErrorService;
    }

    /**
     * @param runId il batch e la giornata, perché lo staging è condiviso con l'ingestione ADX e
     *              senza una chiave riconoscibile una riga FDR sarebbe indistinguibile dalle altre
     * @return quante righe sono state registrate
     */
    public long stage(String runId, FdrBlobFile file, List<FdrRowRejection> parseRejections,
                      List<FdrPersistenceError> persistenceErrors) {
        List<StagingErrorService.DiscardedInputRecord> records = new ArrayList<>();
        for (FdrRowRejection rejection : parseRejections) {
            records.add(new StagingErrorService.DiscardedInputRecord(
                    sourceKey(file, "line", rejection.lineNumber()),
                    payload(file, "line", rejection.lineNumber(), rejection.column()),
                    "FDR_PARSE_" + rejection.reason().name()));
        }
        for (FdrPersistenceError error : persistenceErrors) {
            records.add(new StagingErrorService.DiscardedInputRecord(
                    sourceKey(file, "row", error.rowOrdinal()),
                    payload(file, "row", error.rowOrdinal(), error.sqlState()),
                    "FDR_PERSISTENCE_" + error.category().name()));
        }
        if (records.isEmpty()) {
            return 0;
        }

        // ENTITY_NAME e' VARCHAR e non passa da EntityName.valueOf su questo percorso: il dataset
        // FDR ci sta senza toccare l'enum dell'ingestione ADX, che FDR non attraversa.
        RunContext context = new RunContext(file.dataset().name(), runId, Instant.now());
        context.setOperationId(file.blobName());
        return stagingErrorService.insertDiscardedBulk(context, records);
    }

    private static String sourceKey(FdrBlobFile file, String kind, long position) {
        return file.blobName() + "#" + kind + "=" + position;
    }

    /**
     * Il minimo per ritrovare la riga, non una sua copia.
     *
     * <p>Il file è ancora sul blob e la posizione dice dove guardare: copiare qui i valori
     * significherebbe spargere identificativi in una tabella che nessuno ha incluso nel perimetro
     * del dato, e che ha una retention sua.
     */
    private static Map<String, Object> payload(FdrBlobFile file, String kind, long position,
                                               String detail) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("dataset", file.dataset().name());
        payload.put("business_day", file.businessDay().toString());
        payload.put("blob", file.blobName());
        payload.put(kind, position);
        if (detail != null) {
            payload.put("detail", detail);
        }
        return payload;
    }
}
