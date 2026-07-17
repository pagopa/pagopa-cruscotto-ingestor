package it.pagopa.cruscotto.ingestion.service.ingestion;

import java.time.LocalDateTime;
import java.time.LocalDate;
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
     * Cache lookup puntuale POSITION: chiave = (NAV|PA|INSERTED_TIMESTAMP), valore = ID o null (miss).
     * Evita query duplicate al DB per la stessa lookup nello stesso run.
     */
    private final Map<String, Integer> positionLookupCache = new HashMap<>();
    /**
     * Cache lookup puntuale POSITION per fallback su DATE_EVENT.
     */
    private final Map<String, Integer> positionByDateLookupCache = new HashMap<>();
    /**
     * Cache lookup canonical TOKEN (token -> id o miss).
     */
    private final Map<String, Integer> tokenCanonicalLookupCache = new HashMap<>();
    /**
     * Cache della FK_POSITION associata al TOKEN canonico (token -> fkPosition o null).
     * Popolata quando la risoluzione canonica carica la riga POSITION_TOKENS: evita
     * una findById full-scan sul percorso di cache-hit degli eventi ripetuti.
     */
    private final Map<String, Integer> tokenCanonicalFkPositionCache = new HashMap<>();
    /**
     * Cache lookup TOKEN+DATE_EVENT (token,date -> id o miss).
     */
    private final Map<String, Integer> tokenByDateLookupCache = new HashMap<>();
    /**
     * Cache lookup TOKEN via (fkPosition, iuv, dateEvent) (-> id o miss).
     */
    private final Map<String, Integer> tokenByPositionIuvLookupCache = new HashMap<>();

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

        if (id > 0) {
            positions.removeIf(p -> p.id < 0 && p.insertedTimestamp.equals(insertedTimestamp));
        }

        // Rimuovere duplicato se esiste (update di un record già cachato)
        positions.removeIf(p -> p.id.equals(id));

        // Aggiungere e riordinare DESC
        positions.add(new CachedPosition(id, insertedTimestamp));
        positions.sort((p1, p2) -> p2.insertedTimestamp.compareTo(p1.insertedTimestamp));
        positionLookupCache.put(positionLookupKey(nav, paEmittente, insertedTimestamp), id);
    }

    public boolean hasPositionLookupResult(String nav, String paEmittente, LocalDateTime insertedTs) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return false;
        }
        return positionLookupCache.containsKey(positionLookupKey(nav, paEmittente, insertedTs));
    }

    public Integer getPositionLookupResult(String nav, String paEmittente, LocalDateTime insertedTs) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return null;
        }
        return positionLookupCache.get(positionLookupKey(nav, paEmittente, insertedTs));
    }

    public void cachePositionLookupResult(String nav, String paEmittente, LocalDateTime insertedTs, Integer id) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return;
        }
        positionLookupCache.put(positionLookupKey(nav, paEmittente, insertedTs), id);
    }

    public boolean hasPositionByDateLookupResult(String nav, String paEmittente, LocalDate dateEvent) {
        if (nav == null || paEmittente == null || dateEvent == null) {
            return false;
        }
        return positionByDateLookupCache.containsKey(positionByDateLookupKey(nav, paEmittente, dateEvent));
    }

    public Integer getPositionByDateLookupResult(String nav, String paEmittente, LocalDate dateEvent) {
        if (nav == null || paEmittente == null || dateEvent == null) {
            return null;
        }
        return positionByDateLookupCache.get(positionByDateLookupKey(nav, paEmittente, dateEvent));
    }

    public void cachePositionByDateLookupResult(String nav, String paEmittente, LocalDate dateEvent, Integer id) {
        if (nav == null || paEmittente == null || dateEvent == null) {
            return;
        }
        positionByDateLookupCache.put(positionByDateLookupKey(nav, paEmittente, dateEvent), id);
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

    public boolean hasTokenCanonicalLookupResult(String tokenBase64) {
        if (tokenBase64 == null) {
            return false;
        }
        return tokenCanonicalLookupCache.containsKey(tokenBase64);
    }

    public Integer getTokenCanonicalLookupResult(String tokenBase64) {
        if (tokenBase64 == null) {
            return null;
        }
        return tokenCanonicalLookupCache.get(tokenBase64);
    }

    public void cacheTokenCanonicalLookupResult(String tokenBase64, Integer id) {
        if (tokenBase64 == null) {
            return;
        }
        tokenCanonicalLookupCache.put(tokenBase64, id);
        if (id != null) {
            tokenCache.put(tokenBase64, id);
        }
    }

    /**
     * Verifica se per il TOKEN canonico è già nota la FK_POSITION in cache.
     */
    public boolean hasTokenCanonicalFkPosition(String tokenBase64) {
        if (tokenBase64 == null) {
            return false;
        }
        return tokenCanonicalFkPositionCache.containsKey(tokenBase64);
    }

    /**
     * Ritorna la FK_POSITION cachata per il TOKEN canonico (può essere null).
     */
    public Integer getTokenCanonicalFkPosition(String tokenBase64) {
        if (tokenBase64 == null) {
            return null;
        }
        return tokenCanonicalFkPositionCache.get(tokenBase64);
    }

    /**
     * Registra la FK_POSITION associata al TOKEN canonico.
     */
    public void cacheTokenCanonicalFkPosition(String tokenBase64, Integer fkPosition) {
        if (tokenBase64 == null) {
            return;
        }
        tokenCanonicalFkPositionCache.put(tokenBase64, fkPosition);
    }

    public boolean hasTokenByDateLookupResult(String tokenBase64, LocalDate dateEvent) {
        if (tokenBase64 == null || dateEvent == null) {
            return false;
        }
        return tokenByDateLookupCache.containsKey(tokenByDateLookupKey(tokenBase64, dateEvent));
    }

    public Integer getTokenByDateLookupResult(String tokenBase64, LocalDate dateEvent) {
        if (tokenBase64 == null || dateEvent == null) {
            return null;
        }
        return tokenByDateLookupCache.get(tokenByDateLookupKey(tokenBase64, dateEvent));
    }

    public void cacheTokenByDateLookupResult(String tokenBase64, LocalDate dateEvent, Integer id) {
        if (tokenBase64 == null || dateEvent == null) {
            return;
        }
        tokenByDateLookupCache.put(tokenByDateLookupKey(tokenBase64, dateEvent), id);
        if (id != null) {
            tokenCache.put(tokenBase64, id);
        }
    }

    public boolean hasTokenByPositionIuvLookupResult(Integer fkPosition, String iuv, LocalDate dateEvent) {
        if (fkPosition == null || iuv == null || dateEvent == null) {
            return false;
        }
        return tokenByPositionIuvLookupCache.containsKey(tokenByPositionIuvLookupKey(fkPosition, iuv, dateEvent));
    }

    public Integer getTokenByPositionIuvLookupResult(Integer fkPosition, String iuv, LocalDate dateEvent) {
        if (fkPosition == null || iuv == null || dateEvent == null) {
            return null;
        }
        return tokenByPositionIuvLookupCache.get(tokenByPositionIuvLookupKey(fkPosition, iuv, dateEvent));
    }

    public void cacheTokenByPositionIuvLookupResult(Integer fkPosition, String iuv, LocalDate dateEvent, Integer id) {
        if (fkPosition == null || iuv == null || dateEvent == null) {
            return;
        }
        tokenByPositionIuvLookupCache.put(tokenByPositionIuvLookupKey(fkPosition, iuv, dateEvent), id);
    }

    /**
     * Window-scoped POSITION prefetch: cleared after each ADX window transform.
     * Only positive (non-null) results are stored here. Absence means "not prefetched",
     * so the individual resolver falls back to its own DB query.
     */
    private final Map<String, Integer> positionWindowPrefetch = new HashMap<>();

    /**
     * Window-scoped TOKEN canonical prefetch: cleared after each ADX window transform.
     * Only positive (non-null) results are stored here.
     */
    private final Map<String, Integer> tokenWindowPrefetch = new HashMap<>();

    public boolean hasPositionWindowPrefetch(String nav, String paEmittente, LocalDateTime insertedTs) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return false;
        }
        return positionWindowPrefetch.containsKey(positionLookupKey(nav, paEmittente, insertedTs));
    }

    public Integer getPositionWindowPrefetch(String nav, String paEmittente, LocalDateTime insertedTs) {
        if (nav == null || paEmittente == null || insertedTs == null) {
            return null;
        }
        return positionWindowPrefetch.get(positionLookupKey(nav, paEmittente, insertedTs));
    }

    /** Stores a positive (non-null) prefetch result. Null id values are silently ignored. */
    public void putPositionWindowPrefetch(String nav, String paEmittente, LocalDateTime insertedTs, Integer id) {
        if (nav == null || paEmittente == null || insertedTs == null || id == null) {
            return;
        }
        positionWindowPrefetch.put(positionLookupKey(nav, paEmittente, insertedTs), id);
    }

    public boolean hasTokenWindowPrefetch(String tokenBase64) {
        if (tokenBase64 == null) {
            return false;
        }
        return tokenWindowPrefetch.containsKey(tokenBase64);
    }

    public Integer getTokenWindowPrefetch(String tokenBase64) {
        if (tokenBase64 == null) {
            return null;
        }
        return tokenWindowPrefetch.get(tokenBase64);
    }

    /** Stores a positive (non-null) prefetch result. Null id values are silently ignored. */
    public void putTokenWindowPrefetch(String tokenBase64, Integer id) {
        if (tokenBase64 == null || id == null) {
            return;
        }
        tokenWindowPrefetch.put(tokenBase64, id);
    }

    /**
     * Clears only the window-scoped prefetch maps; called after each ADX window transform
     * to keep memory bounded. Run-level caches are NOT affected.
     */
    public void clearWindowPrefetch() {
        positionWindowPrefetch.clear();
        tokenWindowPrefetch.clear();
    }

    /**
     * Pulisci la cache (fine del batch).
     */
    public void clear() {
        positionCache.clear();
        tokenCache.clear();
        positionLookupCache.clear();
        positionByDateLookupCache.clear();
        tokenCanonicalLookupCache.clear();
        tokenCanonicalFkPositionCache.clear();
        tokenByDateLookupCache.clear();
        tokenByPositionIuvLookupCache.clear();
        positionWindowPrefetch.clear();
        tokenWindowPrefetch.clear();
    }

    private static String positionLookupKey(String nav, String paEmittente, LocalDateTime insertedTs) {
        return nav + "|" + paEmittente + "|" + insertedTs;
    }

    private static String positionByDateLookupKey(String nav, String paEmittente, LocalDate dateEvent) {
        return nav + "|" + paEmittente + "|" + dateEvent;
    }

    private static String tokenByDateLookupKey(String tokenBase64, LocalDate dateEvent) {
        return tokenBase64 + "|" + dateEvent;
    }

    private static String tokenByPositionIuvLookupKey(Integer fkPosition, String iuv, LocalDate dateEvent) {
        return fkPosition + "|" + iuv + "|" + dateEvent;
    }
}
