package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector.Field;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector.TemplateDetection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Validates an uploaded Massive Search CSV and detects its template, streaming the source line by
 * line so large files are never loaded fully in memory.
 *
 * <p>This step only validates and normalizes rows; it does not generate any report.</p>
 */
@Slf4j
@Service
public class MassiveSearchCsvValidator {

    private final CsvTemplateDetector templateDetector;
    private final CsvInputReader inputReader;
    private final int maxErrors;

    public MassiveSearchCsvValidator(
        CsvTemplateDetector templateDetector,
        CsvInputReader inputReader,
        MassiveSearchProperties properties
    ) {
        this.templateDetector = templateDetector;
        this.inputReader = inputReader;
        this.maxErrors = properties.getCsv().getMaxValidationErrors();
    }

    /** Validates the CSV without retaining rows. */
    public CsvValidationResult validate(InputStream inputStream) {
        return validate(inputStream, null);
    }

    /**
     * Validates the CSV and, when a consumer is provided, streams each valid normalized row to it.
     *
     * @param inputStream      the CSV source (closed by the caller)
     * @param validRowConsumer optional consumer invoked for every valid row (may be {@code null})
     * @return the validation result
     */
    public CsvValidationResult validate(InputStream inputStream, Consumer<SearchInputRow> validRowConsumer) {
        List<CsvValidationError> errors = new ArrayList<>();

        try {
            BufferedReader reader = inputReader.newReader(inputStream);

            String headerLine = readHeaderLine(reader);
            if (headerLine == null) {
                addError(errors, CsvValidationError.row(1, "Missing header row"));
                return invalidResult(CsvTemplate.UNKNOWN, errors);
            }

            List<String> headerColumns = inputReader.parseLine(headerLine);
            TemplateDetection detection = templateDetector.detect(headerColumns);
            boolean headerValid = validateHeader(detection, errors);

            if (detection.template() == CsvTemplate.UNKNOWN) {
                addError(errors, CsvValidationError.row(1, "Unrecognized CSV template from header"));
                return invalidResult(CsvTemplate.UNKNOWN, errors);
            }

            Set<Field> requiredFields = requiredFields(detection.template());
            int expectedColumnCount = headerColumns.size();

            long totalRows = 0;
            long validRows = 0;
            long invalidRows = 0;
            String line;
            long lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;

                if (inputReader.isBlank(line)) {
                    totalRows++;
                    invalidRows++;
                    addError(errors, CsvValidationError.row(lineNumber, "Empty row"));
                    continue;
                }

                totalRows++;
                List<String> columns = inputReader.parseLine(line);
                if (columns.size() != expectedColumnCount) {
                    invalidRows++;
                    addError(errors, CsvValidationError.row(lineNumber,
                        "Unexpected column count: expected " + expectedColumnCount + " but found " + columns.size()));
                    continue;
                }

                SearchInputRow row = inputReader.toRow(detection, columns);
                List<CsvValidationError> rowErrors = validateRow(lineNumber, requiredFields, row);
                if (rowErrors.isEmpty()) {
                    validRows++;
                    if (validRowConsumer != null) {
                        validRowConsumer.accept(row);
                    }
                } else {
                    invalidRows++;
                    rowErrors.forEach(e -> addError(errors, e));
                }
            }

            if (totalRows == 0) {
                addError(errors, CsvValidationError.row(1, "No data rows"));
            }
            boolean valid = headerValid && invalidRows == 0 && totalRows > 0;
            log.info("phase=CSV_VALIDATED template={} valid={} totalRows={} validRows={} invalidRows={} errors={}",
                detection.template(), valid, totalRows, validRows, invalidRows, errors.size());
            return new CsvValidationResult(valid, detection.template(), totalRows, validRows, invalidRows, errors);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read Massive Search CSV", e);
        }
    }

    private CsvValidationResult invalidResult(CsvTemplate template, List<CsvValidationError> errors) {
        log.info("phase=CSV_VALIDATED template={} valid=false totalRows=0 validRows=0 invalidRows=0 errors={}",
            template, errors.size());
        return new CsvValidationResult(false, template, 0, 0, 0, errors);
    }

    private String readHeaderLine(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (!inputReader.isBlank(line)) {
                return line;
            }
        }
        return null;
    }

    private boolean validateHeader(TemplateDetection detection, List<CsvValidationError> errors) {
        boolean valid = true;
        for (String unexpected : detection.unexpectedColumns()) {
            valid = false;
            addError(errors, CsvValidationError.column(1, unexpected, "Unexpected column"));
        }
        for (String duplicate : detection.duplicateColumns()) {
            valid = false;
            addError(errors, CsvValidationError.column(1, duplicate, "Duplicate column"));
        }
        return valid;
    }

    private List<CsvValidationError> validateRow(long lineNumber, Set<Field> requiredFields, SearchInputRow row) {
        List<CsvValidationError> rowErrors = new ArrayList<>();
        for (Field field : requiredFields) {
            if (valueOf(field, row) == null) {
                rowErrors.add(CsvValidationError.column(lineNumber, field.name(), "Missing mandatory value"));
            }
        }
        return rowErrors;
    }

    private String valueOf(Field field, SearchInputRow row) {
        return switch (field) {
            case NAV -> row.nav();
            case PA -> row.pa();
            case IUV -> row.iuv();
            case TOKEN -> row.token();
        };
    }

    private Set<Field> requiredFields(CsvTemplate template) {
        return switch (template) {
            case NAV_PA -> Set.of(Field.NAV, Field.PA);
            case IUV_PA -> Set.of(Field.IUV, Field.PA);
            case NAV -> Set.of(Field.NAV);
            case IUV -> Set.of(Field.IUV);
            case TOKEN -> Set.of(Field.TOKEN);
            case UNKNOWN -> Set.of();
        };
    }

    private void addError(List<CsvValidationError> errors, CsvValidationError error) {
        if (errors.size() < maxErrors) {
            errors.add(error);
        }
    }
}
