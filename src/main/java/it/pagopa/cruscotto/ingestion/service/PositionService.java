package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.entity.Positions;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class PositionService {

    private final PositionsRepository positionsRepository;
    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;

    public List<Positions> getPositionsByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionsRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }

    public List<PositionTokens> getPositionTokensByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionTokensRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }

    public List<PositionTransfers> getPositionTransfersByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionTransfersRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }
}
