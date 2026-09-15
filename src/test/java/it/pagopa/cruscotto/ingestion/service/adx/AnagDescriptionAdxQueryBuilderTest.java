package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnagDescriptionAdxQueryBuilderTest {
    private AnagDescriptionAdxQueryBuilder builder;

    @BeforeEach
    void setUp() {
        AdxTableNamesConfig tableNamesConfig = new AdxTableNamesConfig();
        tableNamesConfig.setTables(Map.of(
                "PA", "PA",
                "PSP", "PSP",
                "INTERMEDIARI_PA", "INTERMEDIARI_PA",
                "INTERMEDIARI_PSP", "INTERMEDIARI_PSP"
        ));
        builder = new AnagDescriptionAdxQueryBuilder(new QueryTemplateLoader(), tableNamesConfig);
    }

    @Test
    void shouldBuildPaLookupQuery() {
        String query = builder.buildPaEmittenteQuery(java.util.List.of("00147990923"));

        assertTrue(query.contains("PA"));
        assertTrue(query.contains("ID_DOMINIO"));
        assertTrue(query.contains("RAGIONE_SOCIALE"));
        assertTrue(query.contains("'00147990923'"));
    }

    @Test
    void shouldEscapeQuotesInCodes() {
        String query = builder.buildPspQuery(java.util.List.of("O'Reilly"));

        assertTrue(query.contains("'O''Reilly'"));
    }

    @Test
    void descriptionQueryFiltersOutEmptyDescriptions() {
        String query = builder.buildPaEmittenteQuery(java.util.List.of("00147990923"));

        assertTrue(query.contains("isnotempty"),
                "the description lookup must drop codes without a RAGIONE_SOCIALE");
    }

    @Test
    void reconcileQueryReturnsExistenceRegardlessOfDescription() {
        // The reconcile/existence query must NOT filter on description: a code present with an empty
        // RAGIONE_SOCIALE must still be returned, otherwise it would be judged absent and deleted.
        String query = builder.buildPaEmittenteReconcileQuery(java.util.List.of("00147990923"));

        assertFalse(query.contains("isnotempty"),
                "the reconcile lookup must not filter on the description");
        assertTrue(query.contains("ID_DOMINIO"));
        assertTrue(query.contains("'00147990923'"));
    }
}
