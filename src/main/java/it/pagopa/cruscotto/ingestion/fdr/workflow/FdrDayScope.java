package it.pagopa.cruscotto.ingestion.fdr.workflow;

/**
 * Quanta parte di una giornata va rifatta.
 *
 * <p>La distinzione esiste per una ragione sola, e non è un'ottimizzazione: la scrittura FDR è un
 * <strong>INSERT fedele</strong>, senza deduplica e senza upsert, perché i duplicati del sorgente
 * devono arrivare in banca dati così come sono. Rielaborare un dataset già scritto quindi non lo
 * aggiorna, lo raddoppia — e lo fa in silenzio, perché X ed N tornano comunque a combaciare.
 */
public enum FdrDayScope {

    /**
     * Riprendi ciò che manca: i dataset già conclusi con successo si riportano e basta.
     *
     * <p>È il caso del retry e della ripresa di una giornata abbandonata. Una giornata può ben
     * finire con {@code FLUSSI_RENDICONTAZIONE} completo e {@code IUV_RENDICONTATI} in attesa di
     * un nuovo tentativo — i due si elaborano in sequenza e il secondo può fallire da solo — ma
     * l'unità di ripresa resta la <em>giornata</em>, non il dataset. Senza questa distinzione il
     * tentativo successivo riscriverebbe anche i FLUSSI.
     */
    RESUME,

    /**
     * Rifai tutto, qualunque cosa risulti già conclusa.
     *
     * <p>È il caso della REINGESTION, e lì è l'unica scelta corretta: le due partizioni giornaliere
     * sono appena state svuotate dal truncate, quindi non c'è nulla da conservare e ogni dataset va
     * riscritto da capo. Saltarne uno perché il tentativo precedente lo aveva concluso lascerebbe
     * la giornata <strong>senza</strong> quei dati, cancellati e mai riscritti.
     */
    REBUILD
}
