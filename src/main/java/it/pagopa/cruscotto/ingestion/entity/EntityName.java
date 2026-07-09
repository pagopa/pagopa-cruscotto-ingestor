package it.pagopa.cruscotto.ingestion.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EntityName {
    POSITION("POSITION"),
    POSITION_TOKENS("POSITION_TOKENS"),
    POSITION_TRANSFERS("POSITION_TRANSFERS"),
    EXTRA_INFO("EXTRA_INFO"),
    EVENTS_WF("EVENTS_WF"),
    ANAG_DESCRIPTION_REFRESH("ANAG_DESCRIPTION_REFRESH"),
    RECONCILIATION("RECONCILIATION");

    private final String value;
}
