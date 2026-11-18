package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * One row of {@code IUV_RENDICONTATI}: one transfer reported inside a revision.
 *
 * <p>Mirrors the thirteen source columns plus {@code dateInserted}. Six of them — {@code idFlusso},
 * {@code dataOraFlusso}, {@code idDominio}, {@code psp}, {@code intPsp}, {@code uniqueId} — repeat
 * what the flow row already says. That denormalisation is a confirmed decision, not an oversight:
 * the searches the backend must serve start from {@code IUV}, {@code IUR} or {@code ID_DOMINIO},
 * and the revision they belong to may not have been ingested yet.
 *
 * @param dateInserted the FDR day, from the blob folder
 * @param iuv          with {@code iur} and {@code idsp}, functionally identifies the transfer
 *                     inside a revision. No unique constraint enforces it: an anomalous source must
 *                     be ingested and seen, not rejected
 * @param importo      a {@link BigDecimal}, for the same reason as the flow total
 * @param codEsito     numeric in the source, and the one column that arrives bare in some files and
 *                     quoted in others
 * @param uniqueId     the revision this transfer belongs to; a logical link, not a foreign key
 * @param insertedTimestamp the source timestamp, preserved verbatim across reingestions
 */
public record FdrIuvRow(
        LocalDate dateInserted,
        String iuv,
        String iur,
        BigDecimal importo,
        Integer codEsito,
        LocalDateTime dataEsitoSingoloPagamento,
        Integer idsp,
        String idFlusso,
        LocalDateTime dataOraFlusso,
        String idDominio,
        String psp,
        String intPsp,
        String uniqueId,
        LocalDateTime insertedTimestamp) implements FdrBusinessRow {

    @Override
    public FdrDataset dataset() {
        return FdrDataset.IUV_RENDICONTATI;
    }
}
