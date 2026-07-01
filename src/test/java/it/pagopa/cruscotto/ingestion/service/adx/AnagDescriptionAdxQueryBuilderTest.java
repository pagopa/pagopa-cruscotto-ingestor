package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
}
