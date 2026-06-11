package it.pagopa.cruscotto.ingestion.service.ingestion;

import java.time.LocalDateTime;
import java.util.*;

/**
 * In-memory cache per tracciare entità inserite durante il batch processing.
 * Utilizzato per risolvere correttamente le dipendenze (POSITION, POSITION_TOKENS)
 * quando record multipli dello stess tipo vengono trasformati nello stesso batch.
 *
 * Problema: Durante la trasformazione di un batch, resolveExistingPositionId()
 * legge dal DB ma i record appena trasformati non sono ancora committati.
 * Questa cache consente di trovare i record appena inseriti in memoria.
 */
public class BatchLocalCache {

    /**
     * Cache per POSITION: chiave = (NAV + "|" + PA_EMITTENTE), valore = list di position (ordinate per inserted_timestamp DESC)
     */
    private final Map<String, List<CachedPosition>> positionCache = new HashMap<>();

    /**
     * Cache per POSITION_TOKENS: chiave = token (base64 string), valore = latest token ID
     */
    private final Map<String, Integer> tokenCache = new HashMap<>();

    /**
     * Record cached di POSITION
     */
    public static class CachedPosition {
        public final Integer id;
        public final LocalDateTime insertedTimestamp;

        public CachedPosition(Integer id, LocalDateTime insertedTimestamp) {
            this.id = id;
            this.insertedTimestamp = insertedTimestamp;
        }
    }

    /**
     * Cerca una POSITION nella cache per (NAV, PA_EMITTENTE, inserted_timestamp) in finestra 24h.
     * Ordine DESC per timestamp: ritorna il record più recente all'interno della finestra.
     *
     * @return ID della POSITION più recente, o null se non trovata
     */
    public Integer findPositionInWindow(String nav, String paEmittente, LocalDateTime insertedTs) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return null;
        }

        String key = nav + "|" + paEmittente;
        List<CachedPosition> positions = positionCache.get(key);
        if (positions == null || positions.isEmpty()) {
            return null;
        }

        LocalDateTime windowStart = insertedTs.minusHours(24);

        // Lista è ordinata DESC, ritorna il primo che cade dentro la finestra
        for (CachedPosition p : positions) {
            if (!p.insertedTimestamp.isBefore(windowStart) && !p.insertedTimestamp.isAfter(insertedTs)) {
                return p.id;
            }
        }
        return null;
    }

    /**
     * Registra una POSITION appena inserita/aggiornata nella cache.
     */
    public void cachePosition(Integer id, String nav, String paEmittente, LocalDateTime insertedTimestamp) {
        if (id == null || nav == null || paEmittente == null || insertedTimestamp == null) {
            return;
        }

        String key = nav + "|" + paEmittente;
        List<CachedPosition> positions = positionCache.computeIfAbsent(key, k -> new ArrayList<>());

        // Rimuovere duplicato se esiste (update di un record già cachato)
        positions.removeIf(p -> p.id.equals(id));

        // Aggiungere e riordinare DESC
        positions.add(new CachedPosition(id, insertedTimestamp));
        positions.sort((p1, p2) -> p2.insertedTimestamp.compareTo(p1.insertedTimestamp));
    }

    /**
     * Cerca un TOKEN nella cache.
     */
    public Integer findToken(String tokenBase64) {
        return tokenCache.get(tokenBase64);
    }

    /**
     * Registra un TOKEN appena inserito nella cache.
     */
    public void cacheToken(String tokenBase64, Integer id) {
        if (tokenBase64 != null && id != null) {
            tokenCache.put(tokenBase64, id);
        }
    }

    /**
     * Pulisci la cache (fine del batch).
     */
    public void clear() {
        positionCache.clear();
        tokenCache.clear();
    }
}

