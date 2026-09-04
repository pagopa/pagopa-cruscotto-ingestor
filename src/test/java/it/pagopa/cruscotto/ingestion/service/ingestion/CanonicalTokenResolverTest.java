package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.repository.PositionTokenRegistryReader;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * POSITION_TOKENS is RANGE-partitioned monthly on DATE_EVENT with partitions pre-created years
 * ahead, so a lookup by TOKEN alone probes every partition (~25 index seeks per row). These tests
 * pin the pruning: the registry is consulted first to hit a single partition, and the unpruned
 * lookup survives only as a last resort so no FK is ever lost.
 */
@ExtendWith(MockitoExtension.class)
class CanonicalTokenResolverTest {

    @Mock
    private PositionTokensRepository positionTokensRepository;

    @Mock
    private PositionTokenRegistryReader positionTokenRegistryReader;

    private CanonicalTokenResolver resolver;

    private final byte[] token = "tok-1".getBytes();

    @BeforeEach
    void setUp() {
        resolver = new CanonicalTokenResolver(positionTokensRepository, positionTokenRegistryReader);
    }

    private PositionTokens row(int id, Integer fkPosition) {
        PositionTokens pt = new PositionTokens();
        pt.setId(id);
        pt.setFkPosition(fkPosition);
        return pt;
    }

    @Test
    void prunesToTheRegistryPartitionAndNeverFallsBack() {
        LocalDate firstDateEvent = LocalDate.parse("2026-08-14");
        when(positionTokenRegistryReader.findFirstDateEventByToken(token))
                .thenReturn(Optional.of(firstDateEvent));
        when(positionTokensRepository.findCanonicalByTokenAndDate(token, firstDateEvent))
                .thenReturn(Optional.of(row(7, 42)));

        Optional<PositionTokens> resolved = resolver.findCanonical(token);

        assertTrue(resolved.isPresent());
        assertEquals(7, resolved.orElseThrow().getId());
        verify(positionTokensRepository).findCanonicalByTokenAndDate(token, firstDateEvent);
        // The unpruned lookup would scan every partition: it must not be reached.
        verify(positionTokensRepository, never()).findCanonicalByToken(any());
    }

    @Test
    void fallsBackToUnprunedLookupWhenTokenIsNotInTheRegistry() {
        // Legacy rows or a purged registry: correctness wins over the partition scan.
        when(positionTokenRegistryReader.findFirstDateEventByToken(token)).thenReturn(Optional.empty());
        when(positionTokensRepository.findCanonicalByToken(token)).thenReturn(Optional.of(row(9, 11)));

        Optional<PositionTokens> resolved = resolver.findCanonical(token);

        assertEquals(9, resolved.orElseThrow().getId());
        verify(positionTokensRepository, never()).findCanonicalByTokenAndDate(any(), any());
    }

    @Test
    void fallsBackWhenTheRegistryDateDoesNotMatchAnyRow() {
        LocalDate stale = LocalDate.parse("2026-01-01");
        when(positionTokenRegistryReader.findFirstDateEventByToken(token)).thenReturn(Optional.of(stale));
        when(positionTokensRepository.findCanonicalByTokenAndDate(token, stale)).thenReturn(Optional.empty());
        when(positionTokensRepository.findCanonicalByToken(token)).thenReturn(Optional.of(row(5, 3)));

        assertEquals(5, resolver.findCanonical(token).orElseThrow().getId());
    }

    @Test
    void findCanonicalIdReturnsTheIdOfTheResolvedRow() {
        LocalDate firstDateEvent = LocalDate.parse("2026-08-14");
        when(positionTokenRegistryReader.findFirstDateEventByToken(token))
                .thenReturn(Optional.of(firstDateEvent));
        when(positionTokensRepository.findCanonicalByTokenAndDate(token, firstDateEvent))
                .thenReturn(Optional.of(row(7, 42)));

        assertEquals(Optional.of(7), resolver.findCanonicalId(token));
    }

    @Test
    void nullTokenResolvesToEmptyWithoutTouchingTheDatabase() {
        assertTrue(resolver.findCanonical(null).isEmpty());
        verify(positionTokenRegistryReader, never()).findFirstDateEventByToken(any());
        verify(positionTokensRepository, never()).findCanonicalByToken(any());
    }
}
