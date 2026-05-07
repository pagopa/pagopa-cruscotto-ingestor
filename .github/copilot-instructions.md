# Repository Copilot Instructions (Ingestor – Quartz → Spring Batch)

> These instructions are read by GitHub Copilot before generating suggestions in this repository.

## Ground rules (must follow)

1. **Do not create parallel implementations.** Integrate with existing code and update existing classes/configs when present.
2. **Quartz is the scheduler for ingestion jobs.** Do **not** use Spring `@Scheduled` for ingestion scheduling.
3. **Spring Batch executes the work.** Quartz jobs launch Spring Batch jobs via `JobLauncher.run(Job, JobParameters)`.
4. **Concurrency rule:** prevent concurrent execution of the **same Quartz job (same JobKey / same entity)** using `@DisallowConcurrentExecution`. Different entities may run concurrently.
5. **No hardcoded environment values or secrets.** Never hardcode URLs, credentials, schema names, table prefixes, or cron expressions. Use configuration/properties.
6. **Make code compile-first.** If something is missing, add minimal stubs with `TODO` rather than inventing behavior.
7. **Keep changes minimal.** Prefer small, focused diffs. Avoid large rewrites.

## Project-specific architectural pattern

- **Quartz → Batch:** `QuartzJobBean` launches a Spring Batch `Job` with `JobLauncher` and `JobParameters`.
- **Batch jobs per entity:** prefer 1 Batch job per entity (POSITION, POSITION_TOKENS, POSITION_TRANSFERS, EXTRA_INFO, EVENTS_WF) with a single Tasklet step delegating to an entity runner.
- **Run correlation:** JobParameters must include `runId` (UUID) and `entityName` and they must be propagated to logs.

## Database & Liquibase

- Default schema is `ingestor` (configured via Spring/JPA/Liquibase). Respect existing schema and prefixes.
- Do not modify existing Quartz (`QRTZ_`) and Spring Batch (`BATCH_`) schema objects; **only add** missing objects.
- Any new tables must be added via Liquibase changeSets including rollback.

## ADX / Kusto ingestion rules

- Windowed reads; default initial window is 5 minutes.
- If ADX returns a partial failure / result-set-too-large (64MB) condition, halve the window and retry up to `maxWindowHalvingAttempts`.
- If max attempts exceeded: fail the run with a clear exception and structured logs.

## Logging & observability

- Every log line must include **runId** and **entityName**.
- Log phases: `START`, `END`, `WINDOW`, `CHECKPOINT`, `NOOP`, `ERROR`.
- Ensure `END` is logged even on exceptions (use try/finally).

## Output expectations for Copilot

- Prefer generating **code only**.
- When multiple files are needed, list file names first, then provide code for each file.
- Avoid verbose explanations; keep to short inline comments where needed.
