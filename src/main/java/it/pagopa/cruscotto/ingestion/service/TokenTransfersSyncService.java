package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Servizio per sincronizzare POSITION_TOKENS e POSITION_TRANSFERS.
 * Implementa regola 7.3.1 per pspNotifyPayment:
 * - Quando PSP, INTERMEDIARIO_PSP, CANALE vengono aggiornati su TOKEN,
 *   gli stessi campi devono essere aggiornati su TRANSFERS usando TOKEN come chiave.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TokenTransfersSyncService {

    private final PositionTransfersRepository positionTransfersRepository;
    private final JdbcTemplate jdbcTemplate;
    private final String schema = "ingestor";

    /**
     * Sincronizzare POSITION_TRANSFERS dopo aggiornamento di POSITION_TOKENS.
     * Regola 7.3.1 pspNotifyPayment:
     * Se TOKEN è stato aggiornato con PSP, INTERMEDIARIO_PSP, CANALE,
     * applicare gli stessi campi a tutti i TRANSFERS associati al TOKEN.
     */
    @Transactional
    public void syncTransfersFromTokenUpdate(RunContext ctx, PositionTokens updatedToken) {
        if (updatedToken == null || updatedToken.getId() == null || updatedToken.getToken() == null) {
            return;
        }

        String runId = ctx.getRunId();
        byte[] tokenBytes = updatedToken.getToken();

        try {
            // Cercare tutti i POSITION_TRANSFERS associati a questo TOKEN
            String sql = "UPDATE " + schema + ".POSITION_TRANSFERS " +
                    "SET PSP = ?, INTERMEDIARIO_PSP = ?, CANALE = ? " +
                    "WHERE FK_TOKEN = (SELECT ID FROM " + schema + ".POSITION_TOKENS WHERE TOKEN = ? LIMIT 1)";

            int updated = jdbcTemplate.update(sql,
                    updatedToken.getPsp() != null ? updatedToken.getPsp().intValue() : null,
                    updatedToken.getIntermediarioPsp() != null ? updatedToken.getIntermediarioPsp().intValue() : null,
                    updatedToken.getCanale() != null ? updatedToken.getCanale().intValue() : null,
                    tokenBytes);

            if (updated > 0) {
                try (MdcScope ignored = MdcScope.run(runId, "POSITION_TRANSFERS", "TRANSFER_SYNC");
                     LogDetails details = LogDetails.open()
                             .with(LogFields.ROWS, updated)
                             .with("psp", String.valueOf(updatedToken.getPsp()))
                             .with("intermediario_psp", String.valueOf(updatedToken.getIntermediarioPsp()))
                             .with("canale", String.valueOf(updatedToken.getCanale()))) {
                    log.debug("Synchronized transfers from a token update");
                }
            }
        } catch (Exception e) {
            try (MdcScope ignored = MdcScope.run(runId, "POSITION_TRANSFERS", "TRANSFER_SYNC")
                         .with(LogFields.EVENT_OUTCOME, "failure");
                 LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                log.error("Failed to synchronize transfers from a token update", e);
            }
            // Non lanciare eccezione: questa è una operazione best-effort
        }
    }

    /**
     * Sincronizzare POSITION_TRANSFERS per batch di POSITION_TOKENS aggiornati.
     */
    @Transactional
    public void syncTransfersFromTokenUpdates(RunContext ctx, List<PositionTokens> updatedTokens) {
        if (updatedTokens == null || updatedTokens.isEmpty()) {
            return;
        }

        for (PositionTokens token : updatedTokens) {
            // Solo se il TOKEN è stato effettivamente aggiornato (non nuovo insert)
            if (token.getId() != null) {
                syncTransfersFromTokenUpdate(ctx, token);
            }
        }
    }
}

