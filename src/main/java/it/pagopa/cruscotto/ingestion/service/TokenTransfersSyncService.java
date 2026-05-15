package it.pagopa.cruscotto.ingestion.service;

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
                log.debug("[{}] [TRANSFER_SYNC] Synchronized {} POSITION_TRANSFERS from TOKEN update: " +
                        "psp={} intermediarioPsp={} canale={}",
                        runId, updated,
                        updatedToken.getPsp(),
                        updatedToken.getIntermediarioPsp(),
                        updatedToken.getCanale());
            }
        } catch (Exception e) {
            log.error("[{}] [TRANSFER_SYNC] Failed to sync POSITION_TRANSFERS: {}",
                    runId, e.getMessage(), e);
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

