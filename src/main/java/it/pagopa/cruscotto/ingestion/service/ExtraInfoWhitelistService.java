package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Slf4j
@Service
public class ExtraInfoWhitelistService {

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    private final Object cacheLock = new Object();
    private volatile LocalDate cacheDay = null;
    private volatile Set<String> cachedAllowedInfoNames = Set.of();

    public ExtraInfoWhitelistService(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    public boolean isAllowed(String infoName) {
        if (infoName == null || infoName.isBlank()) {
            return false;
        }
        return getDailyAllowedInfoNames().contains(normalize(infoName));
    }

    private Set<String> getDailyAllowedInfoNames() {
        LocalDate todayUtc = LocalDate.now(ZoneOffset.UTC);
        LocalDate snapshotDay = cacheDay;
        if (todayUtc.equals(snapshotDay)) {
            return cachedAllowedInfoNames;
        }

        synchronized (cacheLock) {
            if (!todayUtc.equals(cacheDay)) {
                cachedAllowedInfoNames = loadAllowedInfoNamesFromDb();
                cacheDay = todayUtc;
                log.info("Loaded EXTRA_INFO whitelist from DB for day={} values={}", todayUtc, cachedAllowedInfoNames.size());
            }
            return cachedAllowedInfoNames;
        }
    }

    private Set<String> loadAllowedInfoNamesFromDb() {
        String sql = "SELECT info_name FROM " + schema + ".extra_info_whitelist";
        List<String> values = jdbc.queryForList(sql, new MapSqlParameterSource(), String.class);
        Set<String> normalized = new LinkedHashSet<>();
        for (String value : values) {
            if (value == null || value.isBlank()) {
                continue;
            }
            normalized.add(normalize(value));
        }
        return Set.copyOf(normalized);
    }

    private String normalize(String value) {
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
