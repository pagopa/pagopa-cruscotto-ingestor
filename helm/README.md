# Helm config for cruscotto-ingestor

This folder mirrors the structure used by `pagopa-cruscotto-backend`.

## Files

- `Chart.yaml`: chart metadata and dependency on `microservice-chart`.
- `Chart.lock`: locked dependency digest/version.
- `values-dev.yaml`: dev environment values.
- `values-uat.yaml`: uat environment values.
- `values-prod.yaml`: prod environment values.

## Notes

- Values use placeholders for secrets (`envSecret`) and require matching keys in Key Vault.
- Image repository/tag should be aligned with your CI release pipeline.
- Probes are configured on `/actuator/health`.
- Values are in **strict mode**: only environment variables actually consumed by Spring/ingestion config are included.

## Strict env mapping

| Env var | Spring property |
| --- | --- |
| `SPRING_PROFILES_ACTIVE` | `spring.profiles.active` |
| `SERVER_PORT` | `server.port` |
| `SPRING_DATASOURCE_URL` | `spring.datasource.url` |
| `SPRING_DATASOURCE_USERNAME` | `spring.datasource.username` |
| `SPRING_DATASOURCE_PASSWORD` | `spring.datasource.password` |
| `SPRING_LIQUIBASE_ENABLED` | `spring.liquibase.enabled` |
| `SPRING_BATCH_JOB_ENABLED` | `spring.batch.job.enabled` |
| `INGESTION_DATABASE_SCHEMA` | `ingestion.database.schema` |
| `AZURE_KUSTO_CLUSTER_URL` | `azure.kusto.cluster.url` |
| `AZURE_KUSTO_TENANT_ID` | `azure.kusto.tenant.id` |
| `AZURE_KUSTO_DATABASE_NAME` | `azure.kusto.database.name` |
| `AZURE_KUSTO_APP_ID` | `azure.kusto.app.id` |
| `AZURE_KUSTO_APP_KEY` | `azure.kusto.app.key` |
| `INGESTION_ADX_ENDPOINT` | `ingestion.adx.endpoint` |
| `INGESTION_ADX_DATABASE` | `ingestion.adx.database` |

## Quick validation

```bash
helm dependency update helm
helm lint helm -f helm/values-dev.yaml
helm lint helm -f helm/values-uat.yaml
helm lint helm -f helm/values-prod.yaml
```


