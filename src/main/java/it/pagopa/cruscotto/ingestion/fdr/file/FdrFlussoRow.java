package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * One row of {@code FLUSSI_RENDICONTAZIONE}: one revision of one reporting flow.
 *
 * <p>Mirrors the twelve source columns plus {@code dateInserted}. It deliberately does not carry an
 * {@code id}: that comes from the sequence at insert time, and a row read from a file has not been
 * given one yet.
 *
 * @param dateInserted      the FDR day, from the blob folder — not from {@code ID_FLUSSO}, whose
 *                          leading date is one PSP's naming convention and not a guaranteed part of
 *                          the format
 * @param insertedTimestamp the timestamp carried by the <em>source</em>. A reingestion must
 *                          reproduce it unchanged; the instant at which we processed the file
 *                          belongs to the operational tables
 * @param idFlusso          the flow family, repeated across its revisions
 * @param causale           kept whole here. The customer asked for it to be shortened to 30
 *                          characters, and that happens where it is written, not where it is read:
 *                          roughly one row in five is affected and comparing what was stored with
 *                          what the file held must stay possible
 * @param sommaVersata      a {@link BigDecimal}: the scale the source wrote is part of the amount
 * @param uniqueId          identifies the revision. It is the logical link {@code IUV_RENDICONTATI}
 *                          carries, with no physical foreign key behind it
 * @param allDates          left as the raw source text. Its business meaning is not settled, so
 *                          nothing here parses, splits or validates it
 */
public record FdrFlussoRow(
        LocalDate dateInserted,
        LocalDateTime insertedTimestamp,
        String idFlusso,
        LocalDateTime dataOraFlusso,
        LocalDateTime dataRegolamento,
        String causale,
        Integer numPagamenti,
        BigDecimal sommaVersata,
        String idDominio,
        String psp,
        String intPsp,
        String uniqueId,
        String allDates) implements FdrBusinessRow {

    @Override
    public FdrDataset dataset() {
        return FdrDataset.FLUSSI_RENDICONTAZIONE;
    }
}
