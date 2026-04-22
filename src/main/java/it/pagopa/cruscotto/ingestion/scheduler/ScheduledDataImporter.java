package it.pagopa.cruscotto.ingestion.scheduler;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import it.pagopa.cruscotto.ingestion.entity.*;
import it.pagopa.cruscotto.ingestion.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScheduledDataImporter {

    private final Client kustoClient;
    private final PositionsRepository positionsRepository;
    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;
    private final EventsWfRepository eventsWfRepository;
    private final ExtraInfoRepository extraInfoRepository;

    @Value("${azure.kusto.database.name}")
    private String databaseName;

    // Configura quanti record estrarre da Azure ad ogni query, recuperabile da application.yml
    @Value("${azure.kusto.batch.size:100}")
    private int batchSize;


    @Scheduled(cron = "0 */2 * * * *")
    @Transactional
    public void importDataFromKusto() {
        log.info("Starting Kusto import task for the last 5 minutes with batch size {}", batchSize);
        // Calcola il tempo esatto una sola volta all'inizio del batch, per evitare slittamenti della finestra temporale se il job dovesse durare più minuti
        // Inizio dal 27 marzo 2026 come richiesto
        Instant startTime = Instant.parse("2026-03-27T00:00:00Z");

        try {
            // Esecuzione sequenziale per verifica funzionamento
            importPositions(startTime, 0, 1);
            importPositionTokens(startTime, 0, 1);
            importPositionTransfers(startTime, 0, 1);
            importEventsWf(startTime, 0, 1);
            importExtraInfo(startTime, 0, 1);
        } catch (Exception e) {
            log.error("Error importing data from Kusto", e);
        }

        log.info("Kusto import task completed.");
    }

    private void importPositions(Instant startTime, int threadIndex, int totalThreads) throws Exception {
        String lastUniqueId = "";
        boolean hasMore = true;

        log.info("Thread {}/{} starting import for POSITIONS bucket", threadIndex + 1, totalThreads);

        while (hasMore) {
            String query = String.format("P_SERT_POSITION | where ingestion_time() > datetime(%s) | where hash(tostring(UNIQUE_ID), %d) == %d | where strcmp(tostring(UNIQUE_ID), '%s') > 0 | order by tostring(UNIQUE_ID) asc | take %d", startTime.toString(), totalThreads, threadIndex, lastUniqueId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<Positions> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                String uniqueId = table.getString("UNIQUE_ID");
                if (uniqueId != null) {
                    lastUniqueId = uniqueId;
                    Positions p = new Positions();
                    p.setUniqueId(uniqueId);
                    if (table.getObject("INSERTED_TIMESTAMP") != null) {
                        p.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                    }
                    p.setNav(table.getString("NAV"));
                    p.setPaEmittente(table.getString("PA_EMITTENTE"));
                    p.setIuv(table.getString("IUV"));
                    p.setToken(table.getString("TOKEN"));
                    p.setStazione(table.getString("STAZIONE"));
                    p.setIntermediarioPa(table.getString("INTERMEDIARIO_PA"));
                    p.setPsp(table.getString("PSP"));
                    p.setCanale(table.getString("CANALE"));
                    p.setIntermediarioPsp(table.getString("INTERMEDIARIO_PSP"));
                    p.setOutcome(table.getString("OUTCOME"));
                    p.setFaultCode(table.getString("FAULT_CODE"));
                    p.setSessionId(table.getString("SESSION_ID"));
                    p.setTipoEvento(table.getString("TIPO_EVENTO"));
                    p.setSottoTipoEvento(table.getString("SOTTO_TIPO_EVENTO"));
                    p.setServiceIdentifier(table.getString("SERVICE_IDENTIFIER"));
                    batch.add(p);
                }
            }
            if (!batch.isEmpty()) {
                positionsRepository.saveAll(batch);
                positionsRepository.flush(); // Consolidiamo a DB per pulire la memory cache JPA e liberare RAM
                log.info("Saved {} new POSITIONS records", batch.size());
            }
            //if (count < batchSize) {
            hasMore = false;
            //}
        }
    }

    private void importPositionTokens(Instant startTime, int threadIndex, int totalThreads) throws Exception {
        String lastUniqueId = "";
        boolean hasMore = true;

        log.info("Thread {}/{} starting import for POSITION_TOKENS bucket", threadIndex + 1, totalThreads);

        while (hasMore) {
            String query = String.format("P_SERT_POSITION_TOKENS | where ingestion_time() > datetime(%s) | where hash(tostring(UNIQUE_ID), %d) == %d | where strcmp(tostring(UNIQUE_ID), '%s') > 0 | order by tostring(UNIQUE_ID) asc | take %d", startTime.toString(), totalThreads, threadIndex, lastUniqueId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<PositionTokens> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                String uniqueId = table.getString("UNIQUE_ID");
                if (uniqueId != null) {
                    lastUniqueId = uniqueId;
                    PositionTokens pt = new PositionTokens();
                    pt.setUniqueId(uniqueId);
                    if (table.getObject("INSERTED_TIMESTAMP") != null) {
                        pt.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                    }
                    if (table.getObject("IS_EVENT_MULTI_PAYMENT") != null) {
                        pt.setIsEventMultiPayment(table.getBooleanObject("IS_EVENT_MULTI_PAYMENT"));
                    }
                    pt.setNav(table.getString("NAV"));
                    pt.setPaEmittente(table.getString("PA_EMITTENTE"));
                    pt.setIuv(table.getString("IUV"));
                    pt.setToken(table.getString("TOKEN"));
                    if (table.getObject("AMOUNT") != null) {
                        pt.setAmount(table.getBigDecimal("AMOUNT"));
                    }
                    if (table.getObject("FEE") != null) {
                        pt.setFee(table.getBigDecimal("FEE"));
                    }
                    pt.setIdCarrello(table.getString("ID_CARRELLO"));
                    pt.setStazione(table.getString("STAZIONE"));
                    pt.setIntermediarioPa(table.getString("INTERMEDIARIO_PA"));
                    pt.setPsp(table.getString("PSP"));
                    pt.setCanale(table.getString("CANALE"));
                    pt.setIntermediarioPsp(table.getString("INTERMEDIARIO_PSP"));
                    pt.setOutcome(table.getString("OUTCOME"));
                    pt.setTouchpoint(table.getString("TOUCHPOINT"));
                    pt.setPaymentMethod(table.getString("PAYMENT_METHOD"));
                    pt.setSessionId(table.getString("SESSION_ID"));
                    pt.setTipoEvento(table.getString("TIPO_EVENTO"));
                    pt.setSottoTipoEvento(table.getString("SOTTO_TIPO_EVENTO"));
                    pt.setServiceIdentifier(table.getString("SERVICE_IDENTIFIER"));
                    batch.add(pt);
                }
            }
            if (!batch.isEmpty()) {
                positionTokensRepository.saveAll(batch);
                positionTokensRepository.flush();
                log.info("Saved {} new POSITION_TOKENS records", batch.size());
            }
            //if (count < batchSize) {
            hasMore = false;
            //}
        }
    }

    private void importPositionTransfers(Instant startTime, int threadIndex, int totalThreads) throws Exception {
        String lastUniqueId = "";
        boolean hasMore = true;

        log.info("Thread {}/{} starting import for POSITION_TRANSFERS bucket", threadIndex + 1, totalThreads);

        while (hasMore) {
            String query = String.format("P_SERT_TRANSFERS | where ingestion_time() > datetime(%s) | where hash(tostring(UNIQUE_ID), %d) == %d | where strcmp(tostring(UNIQUE_ID), '%s') > 0 | order by tostring(UNIQUE_ID) asc | take %d", startTime.toString(), totalThreads, threadIndex, lastUniqueId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<PositionTransfers> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                String uniqueId = table.getString("UNIQUE_ID");
                if (uniqueId != null) {
                    lastUniqueId = uniqueId;
                    PositionTransfers ptr = new PositionTransfers();
                    ptr.setUniqueId(uniqueId);
                    if (table.getObject("INSERTED_TIMESTAMP") != null) {
                        ptr.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                    }
                    ptr.setNav(table.getString("NAV"));
                    ptr.setPaEmittente(table.getString("PA_EMITTENTE"));
                    ptr.setIuv(table.getString("IUV"));
                    ptr.setToken(table.getString("TOKEN"));
                    if (table.getObject("ID_TRANSFER") != null) {
                        ptr.setIdTransfer(table.getIntegerObject("ID_TRANSFER"));
                    }
                    if (table.getObject("TRANSFER_AMOUNT") != null) {
                        ptr.setTransferAmount(table.getBigDecimal("TRANSFER_AMOUNT"));
                    }
                    ptr.setPaTransfer(table.getString("PA_TRANSFER"));
                    ptr.setIbanTransfer(table.getString("IBAN_TRANSFER"));
                    if (table.getObject("IS_BOLLO") != null) {
                        ptr.setIsBollo(table.getBooleanObject("IS_BOLLO"));
                    }
                    ptr.setStazione(table.getString("STAZIONE"));
                    ptr.setIntermediarioPa(table.getString("INTERMEDIARIO_PA"));
                    ptr.setPsp(table.getString("PSP"));
                    ptr.setCanale(table.getString("CANALE"));
                    ptr.setIntermediarioPsp(table.getString("INTERMEDIARIO_PSP"));
                    ptr.setOutcome(table.getString("OUTCOME"));
                    ptr.setFaultCode(table.getString("FAULT_CODE"));
                    ptr.setSessionId(table.getString("SESSION_ID"));
                    ptr.setTipoEvento(table.getString("TIPO_EVENTO"));
                    ptr.setSottoTipoEvento(table.getString("SOTTO_TIPO_EVENTO"));
                    ptr.setServiceIdentifier(table.getString("SERVICE_IDENTIFIER"));
                    batch.add(ptr);
                }
            }
            if (!batch.isEmpty()) {
                positionTransfersRepository.saveAll(batch);
                positionTransfersRepository.flush();
                log.info("Saved {} new POSITION_TRANSFERS records", batch.size());
            }
            //if (count < batchSize) {
            hasMore = false;
            //}
        }
    }

    private void importEventsWf(Instant startTime, int threadIndex, int totalThreads) throws Exception {
        String lastUniqueId = "";
        boolean hasMore = true;

        log.info("Thread {}/{} starting import for EVENTS_WF bucket", threadIndex + 1, totalThreads);

        while (hasMore) {
            String query = String.format("P_SERT_EVENTS_WF | where ingestion_time() > datetime(%s) | where hash(tostring(UNIQUE_ID), %d) == %d | where strcmp(tostring(UNIQUE_ID), '%s') > 0 | order by tostring(UNIQUE_ID) asc | take %d", startTime.toString(), totalThreads, threadIndex, lastUniqueId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<EventsWf> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                String uniqueId = table.getString("UNIQUE_ID");
                if (uniqueId != null) {
                    lastUniqueId = uniqueId;
                    EventsWf ewf = new EventsWf();
                    ewf.setUniqueId(uniqueId);
                    if (table.getObject("INSERTED_TIMESTAMP") != null) {
                        ewf.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                    }
                    if (table.getObject("IS_EVENT_MULTI_PAYMENT") != null) {
                        ewf.setIsEventMultiPayment(table.getBooleanObject("IS_EVENT_MULTI_PAYMENT"));
                    }
                    ewf.setNav(table.getString("NAV"));
                    ewf.setPaEmittente(table.getString("PA_EMITTENTE"));
                    ewf.setIuv(table.getString("IUV"));
                    ewf.setCreditorRefId(table.getString("CREDITOR_REF_ID"));
                    ewf.setToken(table.getString("TOKEN"));
                    ewf.setPsp(table.getString("PSP"));
                    ewf.setIntermediarioPsp(table.getString("INTERMEDIARIO_PSP"));
                    ewf.setIntermediarioPa(table.getString("INTERMEDIARIO_PA"));
                    ewf.setCanale(table.getString("CANALE"));
                    ewf.setStazione(table.getString("STAZIONE"));
                    ewf.setOutcome(table.getString("OUTCOME"));
                    ewf.setFaultCode(table.getString("FAULT_CODE"));
                    ewf.setSessionId(table.getString("SESSION_ID"));
                    ewf.setTipoEvento(table.getString("TIPO_EVENTO"));
                    ewf.setSottoTipoEvento(table.getString("SOTTO_TIPO_EVENTO"));
                    ewf.setServiceIdentifier(table.getString("SERVICE_IDENTIFIER"));
                    ewf.setPaymentMethod(table.getString("PAYMENT_METHOD"));
                    batch.add(ewf);
                }
            }
            if (!batch.isEmpty()) {
                eventsWfRepository.saveAll(batch);
                eventsWfRepository.flush();
                log.info("Saved {} new EVENTS_WF records", batch.size());
            }
            //if (count < batchSize) {
            hasMore = false;
            //}
        }
    }

    private void importExtraInfo(Instant startTime, int threadIndex, int totalThreads) throws Exception {
        String lastUniqueId = "";
        boolean hasMore = true;

        log.info("Thread {}/{} starting import for EXTRA_INFO bucket", threadIndex + 1, totalThreads);

        while (hasMore) {
            String query = String.format("P_SERT_EXTRA_INFO | where ingestion_time() > datetime(%s) | where hash(tostring(UNIQUE_ID), %d) == %d | where strcmp(tostring(UNIQUE_ID), '%s') > 0 | order by tostring(UNIQUE_ID) asc | take %d", startTime.toString(), totalThreads, threadIndex, lastUniqueId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<ExtraInfo> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                String uniqueId = table.getString("UNIQUE_ID");
                if (uniqueId != null) {
                    lastUniqueId = uniqueId;
                    ExtraInfo ei = new ExtraInfo();
                    ei.setUniqueId(uniqueId);
                    if (table.getObject("INSERTED_TIMESTAMP") != null) {
                        ei.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                    }
                    if (table.getObject("IS_EVENT_MULTI_PAYMENT") != null) {
                        ei.setIsEventMultiPayment(table.getBooleanObject("IS_EVENT_MULTI_PAYMENT"));
                    }
                    ei.setNav(table.getString("NAV"));
                    ei.setPaEmittente(table.getString("PA_EMITTENTE"));
                    ei.setIuv(table.getString("IUV"));
                    ei.setToken(table.getString("TOKEN"));
                    ei.setIntermediarioPsp(table.getString("INTERMEDIARIO_PSP"));
                    ei.setPsp(table.getString("PSP"));
                    ei.setCanale(table.getString("CANALE"));
                    ei.setOutcome(table.getString("OUTCOME"));
                    ei.setPaymentMethod(table.getString("PAYMENT_METHOD"));
                    ei.setTransactionStatus(table.getString("TRANSACTION_STATUS"));
                    ei.setAdditionalInfo(table.getString("ADDITIONAL_INFO"));
                    ei.setSessionId(table.getString("SESSION_ID"));
                    ei.setTipoEvento(table.getString("TIPO_EVENTO"));
                    ei.setSottoTipoEvento(table.getString("SOTTO_TIPO_EVENTO"));
                    ei.setServiceIdentifier(table.getString("SERVICE_IDENTIFIER"));
                    batch.add(ei);
                }
            }
            if (!batch.isEmpty()) {
                extraInfoRepository.saveAll(batch);
                extraInfoRepository.flush();
                log.info("Saved {} new EXTRA_INFO records", batch.size());
            }
            //if (count < batchSize) {
            hasMore = false;
            //}
        }
    }
}
