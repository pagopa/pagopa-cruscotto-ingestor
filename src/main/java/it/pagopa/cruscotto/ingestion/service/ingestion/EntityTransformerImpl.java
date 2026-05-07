package it.pagopa.cruscotto.ingestion.service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EntityTransformerImpl implements EntityTransformer {

    private final ObjectMapper objectMapper;

    @Override
    public <T> T transform(Map<String, Object> row, Class<T> targetClass) throws TransformationException {
        try {
            // Use ObjectMapper to convert the map to the target class
            return objectMapper.convertValue(row, targetClass);
        } catch (Exception e) {
            String errorMsg = "Failed to transform row to " + targetClass.getSimpleName();
            log.error(errorMsg, e);
            throw new TransformationException(errorMsg, e);
        }
    }
}

