package it.pagopa.cruscotto.ingestion.service.adx;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service to load KustoQL query templates from resource files.
 * Provides caching and placeholder substitution.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryTemplateLoader {
    private final Map<String, String> templateCache = new ConcurrentHashMap<>();

    /**
     * Load a query template and substitute placeholders.
     *
     * @param templateName Template name (e.g., "position", "position_tokens", "events_wf_req_resp")
     * @param placeholders Map of placeholder name -> value (e.g., "start" -> "2024-01-01T00:00:00Z")
     * @return Substituted query string
     */
    public String loadAndSubstitute(String templateName, Map<String, String> placeholders) {
        String template = loadTemplate(templateName);
        if (template == null) {
            throw new IllegalArgumentException("Query template not found: " + templateName);
        }
        return substitute(template, placeholders);
    }

    /**
     * Load a template from classpath resource, with caching.
     *
     * @param templateName Template name without extension (e.g., "position" for "position.kql")
     * @return Template content or null if not found
     */
    private String loadTemplate(String templateName) {
        return templateCache.computeIfAbsent(templateName, this::readTemplate);
    }

    private String readTemplate(String templateName) {
        String resourcePath = "queries/adx/" + templateName + ".kql";
        try {
            ClassPathResource resource = new ClassPathResource(resourcePath);
            try (InputStream inputStream = resource.getInputStream()) {
                String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                log.debug("Loaded query template: {}", templateName);
                return content;
            }
        } catch (IOException e) {
            log.warn("Failed to load query template: {}", templateName, e);
            return null;
        }
    }

    /**
     * Substitute placeholders in template.
     * Placeholders are in format: ${name}
     *
     * @param template Template with placeholders
     * @param placeholders Map of name -> value
     * @return Substituted string
     */
    private String substitute(String template, Map<String, String> placeholders) {
        String result = template;
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            String placeholder = "${" + entry.getKey() + "}";
            String value = entry.getValue();
            if (value == null) {
                value = "";
            }
            result = result.replace(placeholder, value);
        }
        return result;
    }
}

