package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * A ready-to-run perimeter query: the dynamic SQL and its bound named parameters.
 */
public record PerimeterQuery(String sql, MapSqlParameterSource params) {}
