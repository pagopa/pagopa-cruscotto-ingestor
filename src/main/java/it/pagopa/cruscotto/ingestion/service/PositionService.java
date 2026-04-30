package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.entity.DataLayerPositionTokens;
import it.pagopa.cruscotto.ingestion.entity.DataLayerPositionTransfers;
import it.pagopa.cruscotto.ingestion.entity.DataLayerPositions;
import it.pagopa.cruscotto.ingestion.repository.DataLayerPositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.DataLayerPositionTransfersRepository;
import it.pagopa.cruscotto.ingestion.repository.DataLayerPositionsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class PositionService {

    private final DataLayerPositionsRepository positionsRepository;
    private final DataLayerPositionTokensRepository positionTokensRepository;
    private final DataLayerPositionTransfersRepository positionTransfersRepository;

    public List<DataLayerPositions> getPositionsByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionsRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }

    public List<DataLayerPositionTokens> getPositionTokensByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionTokensRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }

    public List<DataLayerPositionTransfers> getPositionTransfersByPaEmittenteAndNav(String paEmittente, String nav) {
        return positionTransfersRepository.findByPaEmittenteAndNav(paEmittente, nav);
    }
}
