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
    
    // Configura quanti record estrarre da Azure ad ogni query, recuperabile da application.properties
    @Value("${azure.kusto.batch.size:10000}")
    private int batchSize;

    @Scheduled(cron = "0 */5 * * * *")
    @Transactional
    public void importDataFromKusto() {
        log.info("Starting Kusto import task for the last 5 minutes with batch size {}", batchSize);
        // Calcola il tempo esatto una sola volta all'inizio del batch, per evitare slittamenti della finestra temporale se il job dovesse durare più minuti   
        Instant startTime = Instant.now().minus(5, ChronoUnit.MINUTES);
        
        try {
            importPositions(startTime);
            importPositionTokens(startTime);
            importPositionTransfers(startTime);
            importEventsWf(startTime);
            importExtraInfo(startTime);
        } catch (Exception e) {
            log.error("Error importing data from Kusto", e);
        }
        log.info("Kusto import task completed.");
    }

    private void importPositions(Instant startTime) throws Exception {
        Long lastId = 0L;
        boolean hasMore = true;

        while (hasMore) {
            String query = String.format("POSITIONS | where ingestion_time() > datetime(%s) | where ID > %d | order by ID asc | take %d", startTime.toString(), lastId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<Positions> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                Long id = table.getLong("ID");
                if (id != null) {
                    lastId = Math.max(lastId, id);
                    if (!positionsRepository.existsById(id)) {
                        Positions p = new Positions();
                        p.setId(id);
                        p.setDateEvent(table.getDate("DATE_EVENT") != null ? LocalDate.parse(table.getString("DATE_EVENT").substring(0, 10)) : null);
                        p.setInsertedTimestamp(table.getKustoDateTime("INSERTED_TIMESTAMP"));
                        p.setNav(table.getString("NAV"));
                        p.setPaEmittente(table.getString("PA_EMITTENTE"));
                        p.setLastEvent(table.getKustoDateTime("LAST_EVENT"));
                        p.setDateEvents(table.getString("DATE_EVENTS"));
                        String uuid = table.getString("UUID_POSITION");
                        p.setUuidPosition(uuid != null ? UUID.fromString(uuid) : null);
                        batch.add(p);
                    }
                }
            }
            if (!batch.isEmpty()) {
                positionsRepository.saveAll(batch);
                positionsRepository.flush(); // Consolidiamo a DB per pulire la memory cache JPA e liberare RAM
                log.info("Saved {} new POSITIONS records", batch.size());
            }
            if (count < batchSize) {
                hasMore = false;
            }
        }
    }

    private void importPositionTokens(Instant startTime) throws Exception {
        Long lastId = 0L;
        boolean hasMore = true;

        while (hasMore) {
            String query = String.format("POSITION_TOKENS | where ingestion_time() > datetime(%s) | where ID > %d | order by ID asc | take %d", startTime.toString(), lastId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<PositionTokens> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                Long  id = table.getLong("ID");
                if (id != null) {
                    lastId = Math.max(lastId, id);
                    if (!positionTokensRepository.existsById(id)) {
                        PositionTokens pt = new PositionTokens();
                        pt.setId(id);
                        pt.setDateEvent(table.getDate("DATE_EVENT") != null ? LocalDate.parse(table.getString("DATE_EVENT").substring(0, 10)) : null);
                        pt.setFkPosition(table.getLong("FK_POSITION"));
                        String tokenStr = table.getString("TOKEN");
                        pt.setToken(tokenStr != null ? tokenStr.getBytes() : null);
                        pt.setAmount(table.getBigDecimal("AMOUNT"));
                        pt.setFee(table.getBigDecimal("FEE"));
                        pt.setIuv(table.getString("IUV"));
                        pt.setCreditorRefId(table.getString("CREDITOR_REF_ID"));
                        pt.setOutcome(table.getString("OUTCOME"));
                        pt.setIdCarrello(table.getShort("ID_CARRELLO"));
                        pt.setStazione(table.getShort("STAZIONE"));
                        pt.setCanale(table.getShort("CANALE"));
                        pt.setIntermediarioPa(table.getShort("INTERMEDIARIO_PA"));
                        pt.setIntermediarioPsp(table.getShort("INTERMEDIARIO_PSP"));
                        pt.setPsp(table.getShort("PSP"));
                        pt.setTouchpoint(table.getString("TOUCHPOINT"));
                        pt.setPaymentMethod(table.getString("PAYMENT_METHOD"));
                        pt.setPaymentDate(table.getKustoDateTime("PAYMENT_DATE"));
                        batch.add(pt);
                    }
                }
            }
            if (!batch.isEmpty()) {
                positionTokensRepository.saveAll(batch);
                positionTokensRepository.flush();
                log.info("Saved {} new POSITION_TOKENS records", batch.size());
            }
            if (count < batchSize) {
                hasMore = false;
            }
        }
    }

    private void importPositionTransfers(Instant startTime) throws Exception {
        Long lastId = 0L;
        boolean hasMore = true;

        while (hasMore) {
            String query = String.format("POSITION_TRANSFERS | where ingestion_time() > datetime(%s) | where ID > %d | order by ID asc | take %d", startTime.toString(), lastId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<PositionTransfers> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                Long id = table.getLong("ID");
                if (id != null) {
                    lastId = Math.max(lastId, id);
                    if (!positionTransfersRepository.existsById(id)) {
                        PositionTransfers ptr = new PositionTransfers();
                        ptr.setId(id);
                        ptr.setDateEvent(table.getDate("DATE_EVENT") != null ? LocalDate.parse(table.getString("DATE_EVENT").substring(0, 10)) : null);
                        ptr.setFkToken(table.getLong("FK_TOKEN"));
                        String paStr = table.getString("PA_TRANSFER");
                        ptr.setPaTransfer(paStr != null ? paStr.getBytes() : null);
                        ptr.setIdTransfer(table.getBigDecimal("ID_TRANSFER"));
                        ptr.setIbanTransfer(table.getString("IBAN_TRANSFER"));
                        ptr.setAmountTransfer(table.getString("AMOUNT_TRANSFER"));
                        ptr.setIsBollo(table.getBoolean("IS_BOLLO"));
                        batch.add(ptr);
                    }
                }
            }
            if (!batch.isEmpty()) {
                positionTransfersRepository.saveAll(batch);
                positionTransfersRepository.flush();
                log.info("Saved {} new POSITION_TRANSFERS records", batch.size());
            }
            if (count < batchSize) {
                hasMore = false;
            }
        }
    }

    private void importEventsWf(Instant startTime) throws Exception {
        Long lastId = 0L;
        boolean hasMore = true;

        while (hasMore) {
            String query = String.format("EVENTS_WF | where ingestion_time() > datetime(%s) | where ID > %d | order by ID asc | take %d", startTime.toString(), lastId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<EventsWf> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                Long id = table.getLong("ID");
                if (id != null) {
                    lastId = Math.max(lastId, id);
                    if (!eventsWfRepository.existsById(id)) {
                        EventsWf ewf = new EventsWf();
                        ewf.setId(id);
                        ewf.setDateEvent(table.getDate("DATE_EVENT") != null ? LocalDate.parse(table.getString("DATE_EVENT").substring(0, 10)) : null);
                        ewf.setFkPositions(table.getLong("FK_POSITIONS"));
                        ewf.setFkTokens(table.getLong("FK_TOKENS"));
                        ewf.setInsertedTimestamp(table.getInt("INSERTED_TIMESTAMP"));
                        ewf.setEventId(table.getString("EVENT_ID"));
                        ewf.setFaultCode(table.getShort("FAULT_CODE"));
                        ewf.setOutcome(table.getString("OUTCOME"));
                        ewf.setTipoEvento(table.getShort("TIPO_EVENTO"));
                        batch.add(ewf);
                    }
                }
            }
            if (!batch.isEmpty()) {
                eventsWfRepository.saveAll(batch);
                eventsWfRepository.flush();
                log.info("Saved {} new EVENTS_WF records", batch.size());
            }
            if (count < batchSize) {
                hasMore = false;
            }
        }
    }

    private void importExtraInfo(Instant startTime) throws Exception {
        Long lastId = 0L;
        boolean hasMore = true;

        while (hasMore) {
            String query = String.format("EXTRA_INFO | where ingestion_time() > datetime(%s) | where ID > %d | order by ID asc | take %d", startTime.toString(), lastId, batchSize);
            KustoOperationResult results = kustoClient.execute(databaseName, query);
            KustoResultSetTable table = results.getPrimaryResults();

            List<ExtraInfo> batch = new ArrayList<>();
            int count = 0;
            while (table.next()) {
                count++;
                Long id = table.getLong("ID");
                if (id != null) {
                    lastId = Math.max(lastId, id);
                    if (!extraInfoRepository.existsById(id)) {
                        ExtraInfo ei = new ExtraInfo();
                        ei.setId(id);
                        ei.setDateEvent(table.getDate("DATE_EVENT") != null ? LocalDate.parse(table.getString("DATE_EVENT").substring(0, 10)) : null);
                        ei.setFkTokens(table.getLong("FK_TOKENS"));
                        ei.setInfoName(table.getString("INFO_NAME"));
                        ei.setInfoValue(table.getString("INFO_VALUE"));
                        ei.setTipoEvento(table.getShort("TIPO_EVENTO"));
                        batch.add(ei);
                    }
                }
            }
            if (!batch.isEmpty()) {
                extraInfoRepository.saveAll(batch);
                extraInfoRepository.flush();
                log.info("Saved {} new EXTRA_INFO records", batch.size());
            }
            if (count < batchSize) {
                hasMore = false;
            }
        }
    }
}
