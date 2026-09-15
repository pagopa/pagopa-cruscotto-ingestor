package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class AnagDescriptionAdxQueryBuilder {
    private final QueryTemplateLoader templateLoader;
    private final AdxTableNamesConfig tableNamesConfig;

    public String buildPaEmittenteQuery(List<String> codes) {
        return buildQuery("PA", "ID_DOMINIO", "RAGIONE_SOCIALE", codes);
    }

    /**
     * Existence query for reconciliation: returns every ID_DOMINIO present in the master, including
     * those with an empty RAGIONE_SOCIALE. Callers use it to tell "absent from master" (deletable)
     * apart from "present but without description" (kept as-is). Uses a dedicated template so the
     * shared description lookup stays untouched.
     */
    public String buildPaEmittenteReconcileQuery(List<String> codes) {
        return buildTemplateQuery("anag_reconcile_lookup", "PA", "ID_DOMINIO", "RAGIONE_SOCIALE", codes);
    }

    public String buildPspQuery(List<String> codes) {
        return buildQuery("PSP", "ID_PSP", "RAGIONE_SOCIALE", codes);
    }

    public String buildIntermediarioPaQuery(List<String> codes) {
        return buildQuery("INTERMEDIARI_PA", "ID_INTERMEDIARIO_PA", "INTERMEDIARIO_DESCR", codes);
    }

    public String buildIntermediarioPspQuery(List<String> codes) {
        return buildQuery("INTERMEDIARI_PSP", "ID_INTERMEDIARIO_PSP", "INTERMEDIARIO_DESCR", codes);
    }

    private String buildQuery(String tableKey, String keyColumn, String descriptionColumn, List<String> codes) {
        return buildTemplateQuery("anag_description_lookup", tableKey, keyColumn, descriptionColumn, codes);
    }

    private String buildTemplateQuery(String template, String tableKey, String keyColumn, String descriptionColumn,
                                      List<String> codes) {
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("table_name", tableNamesConfig.getTableName(tableKey));
        placeholders.put("key_column", keyColumn);
        placeholders.put("description_column", descriptionColumn);
        placeholders.put("codes", toKustoStringList(codes));
        return templateLoader.loadAndSubstitute(template, placeholders);
    }

    private String toKustoStringList(List<String> codes) {
        return codes.stream()
                .map(code -> "'" + code.replace("'", "''") + "'")
                .reduce((left, right) -> left + ", " + right)
                .orElse("''");
    }
}
