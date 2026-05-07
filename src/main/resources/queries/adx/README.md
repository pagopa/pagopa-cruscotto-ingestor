# KustoQL Query Templates

This directory contains KustoQL query templates for ADX data ingestion.

## Directory Structure

```
queries/adx/
├── position.kql              - Query for POSITION entity
├── position_tokens.kql       - Query for POSITION_TOKENS entity
├── transfers.kql             - Query for POSITION_TRANSFERS entity
├── events_wf_req_resp.kql    - Query for EVENTS_WF (REQ/RESP join)
├── events_wf_receipt.kql     - Query for EVENTS_WF (Receipt)
└── extra_info.kql            - Query for EXTRA_INFO entity
```

## Placeholder System

All templates use placeholders in the format `${name}` for dynamic substitution:

| Placeholder | Description | Format |
|:---|:---|:---|
| `${start}` | Query window start time | ISO 8601 (e.g., `2024-01-01T00:00:00Z`) |
| `${end}` | Query window end time | ISO 8601 (e.g., `2024-01-01T00:30:00Z`) |
| `${estimates}` | Optional diagnostics clause | KQL pipeline fragment or empty string |

## Usage

Templates are loaded at runtime by `QueryTemplateLoader` service and substituted with actual values from `RunContext` and configuration.

### Example

Template: `position.kql` with placeholder `${start}`:
```
let start=datetime('${start}');
```

Substitution map: `{"start": "2024-01-01T00:00:00Z"}`

Result:
```
let start=datetime('2024-01-01T00:00:00Z');
```

## Estimates Mode

When `adx.include-estimates=true` is configured, the `${estimates}` placeholder is replaced with:

```kql
| extend DimensioneRiga = estimate_data_size(*)
| summarize NUMERI_RIGHE=count(), SIZE_IN_MB = sum(DimensioneRiga) / 1024 / 1024
```

Otherwise, `${estimates}` stays empty.

## Window Recommendations (Client-Provided)

- **POSITION / POSITION_TOKENS / TRANSFERS**: 30 minutes (1h may exceed 64 MB)
- **EVENTS_WF**: 10 minutes for REQ section; RESP uses +5 minute extension
- **EXTRA_INFO**: 30 minutes

## Maintenance

To modify a query:
1. Edit the corresponding `.kql` file
2. No code changes needed; builders will load updated template on next run
3. Ensure placeholder syntax is preserved (`${name}`)

