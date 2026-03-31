package it.pagopa.cruscotto.ingestion.controller;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.entity.Positions;
import it.pagopa.cruscotto.ingestion.service.PositionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/positions")
@RequiredArgsConstructor
public class PositionController {

    private final PositionService positionService;

    @GetMapping
    public ResponseEntity<List<Positions>> getPositions(
            @RequestParam String paEmittente,
            @RequestParam String nav) {
        return ResponseEntity.ok(positionService.getPositionsByPaEmittenteAndNav(paEmittente, nav));
    }

    @GetMapping("/tokens")
    public ResponseEntity<List<PositionTokens>> getPositionTokens(
            @RequestParam String paEmittente,
            @RequestParam String nav) {
        return ResponseEntity.ok(positionService.getPositionTokensByPaEmittenteAndNav(paEmittente, nav));
    }

    @GetMapping("/transfers")
    public ResponseEntity<List<PositionTransfers>> getPositionTransfers(
            @RequestParam String paEmittente,
            @RequestParam String nav) {
        return ResponseEntity.ok(positionService.getPositionTransfersByPaEmittenteAndNav(paEmittente, nav));
    }

}
