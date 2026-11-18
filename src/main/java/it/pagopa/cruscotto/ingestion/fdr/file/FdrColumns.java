package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.util.List;

/**
 * The CSV column names of the two datasets, and which of them must be present.
 *
 * <p>The names are the customer's, taken verbatim from the header of the production exports. They
 * are not the database column names by coincidence but by the source's choice, and the two are free
 * to diverge: the tables carry {@code DATE_INSERTED} and {@code ID}, which no file contains.
 *
 * <p><strong>Every source column is required.</strong> This is the one place where FDR is strict,
 * and the reason is that the alternative is worse: a missing column cannot be detected row by row,
 * only by its absence from the header, and carrying on would write a whole day of rows with that
 * field silently null. A file whose header does not match is a change of contract and must stop the
 * dataset, loudly, while the data is still on the blob and can be read again.
 */
public final class FdrColumns {

    // ─── FLUSSI_RENDICONTAZIONE ───────────────────────────────────────────────────────────────

    public static final String ID_FLUSSO = "ID_FLUSSO";
    public static final String DATA_ORA_FLUSSO = "DATA_ORA_FLUSSO";
    public static final String INSERTED_TIMESTAMP = "INSERTED_TIMESTAMP";
    public static final String DATA_REGOLAMENTO = "DATA_REGOLAMENTO";
    public static final String CAUSALE = "CAUSALE";
    public static final String NUM_PAGAMENTI = "NUM_PAGAMENTI";
    public static final String SOMMA_VERSATA = "SOMMA_VERSATA";
    public static final String ID_DOMINIO = "ID_DOMINIO";
    public static final String PSP = "PSP";
    public static final String INT_PSP = "INT_PSP";
    public static final String UNIQUE_ID = "UNIQUE_ID";
    public static final String ALL_DATES = "ALL_DATES";

    // ─── IUV_RENDICONTATI ─────────────────────────────────────────────────────────────────────

    public static final String IUV = "IUV";
    public static final String IUR = "IUR";
    public static final String IMPORTO = "IMPORTO";
    public static final String COD_ESITO = "COD_ESITO";
    public static final String DATA_ESITO_SINGOLO_PAGAMENTO = "DATA_ESITO_SINGOLO_PAGAMENTO";
    public static final String IDSP = "IDSP";

    private static final List<String> FLUSSI_REQUIRED = List.of(
            ID_FLUSSO, DATA_ORA_FLUSSO, INSERTED_TIMESTAMP, DATA_REGOLAMENTO, CAUSALE,
            NUM_PAGAMENTI, SOMMA_VERSATA, ID_DOMINIO, PSP, INT_PSP, UNIQUE_ID, ALL_DATES);

    private static final List<String> IUV_REQUIRED = List.of(
            IUV, IUR, IMPORTO, COD_ESITO, DATA_ESITO_SINGOLO_PAGAMENTO, IDSP, ID_FLUSSO,
            DATA_ORA_FLUSSO, ID_DOMINIO, PSP, INT_PSP, UNIQUE_ID, INSERTED_TIMESTAMP);

    private FdrColumns() {
    }

    /**
     * The columns a file of this dataset must contain.
     *
     * <p>The order is the one observed in the exports, but nothing depends on it: the header is
     * read by name. That is the whole point — the two datasets share eight column names in
     * different positions, so a positional reader would quietly swap {@code IUR} and {@code IUV}
     * the day the source reorders them.
     */
    public static List<String> required(FdrDataset dataset) {
        return dataset == FdrDataset.FLUSSI_RENDICONTAZIONE ? FLUSSI_REQUIRED : IUV_REQUIRED;
    }
}
