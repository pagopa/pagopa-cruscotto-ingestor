package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * A ready-to-run perimeter query: the dynamic SQL and its bound named parameters.
 *
 * @param sql    la query composta
 * @param params i parametri nominali
 * @param shape  forma scelta dai filtri ({@code TOKEN}, {@code POSITION}, {@code UNION}); esposta per
 *               finire nella diagnostica dello step, perche' spiega da sola perche' un perimetro e'
 *               piu' grande o piu' lento del previsto
 */
public record PerimeterQuery(String sql, MapSqlParameterSource params, String shape) {}
