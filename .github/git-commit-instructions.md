# Git Commit Instructions – Ingestor Project

These instructions guide GitHub Copilot when generating git commit messages
for this repository.

## Commit message format (mandatory)

Use the following format:

<type>: <short description>

Optional body (only if useful):
- Why the change was made
- High-level impact (no implementation details)

### Allowed types

- feat: new feature or capability
- fix: bug fix
- refactor: code restructure without behavior change
- ingestion: changes related to ingestion logic (jobs, runner, ADX, checkpoint)
- quartz: Quartz jobs, triggers, scheduler configuration
- batch: Spring Batch jobs, steps, tasklets
- db: Liquibase, schema, migrations
- chore: repo configuration, build, tooling, Copilot instructions
- docs: documentation only
- test: tests only

## Style rules

- Use **imperative present tense**
    - ✅ “add quartz jobs for ingestion”
    - ❌ “added quartz jobs”
- Keep the subject line **under 72 characters**
- Do not end the subject line with a period
- Do not include ticket numbers unless explicitly present in the branch name

## Examples (good)

- ingestion: add GenericIngestionRunner skeleton
- quartz: register triggers for 5 ingestion entities
- batch: add Spring Batch jobs per entity
- db: add job_schedules checkpoint table
- chore: add copilot repository instructions

## Examples (bad)

- update stuff
- fix
- WIP
- changes
- adjust logic

## Additional rules

- If multiple areas are touched, prefer the **dominant one**
  (e.g. quartz vs batch → quartz wins if triggers are touched)
- Avoid noisy detail; the commit should explain **intent**, not code lines