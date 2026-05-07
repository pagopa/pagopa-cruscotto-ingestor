# Prompt Playbook – Ingestor (Quartz → Spring Batch)

Questo repository usa **Quartz** per la schedulazione (con persistenza su DB) e **Spring Batch** per l’esecuzione dei job (Job/Step/Tasklet). Nel codice esistente è già presente il pattern **QuartzJobBean → JobLauncher.run(Job Spring Batch)** e la configurazione di Batch/Quartz su PostgreSQL. citeturn118search94turn118search93turn118search92turn118search87turn118search88turn118search89

L’obiettivo dei prompt è generare, in modo incrementale e rilanciabile, l’impianto per **5 job di ingestion** (POSITION, POSITION_TOKENS, POSITION_TRANSFERS, EXTRA_INFO, EVENTS_WF) mantenendo:

- **Quartz** come scheduler (no `@Scheduled` Spring per questi job)
- **@DisallowConcurrentExecution** sui Quartz job per evitare due run concorrenti dello stesso job
- **Spring Batch** per orchestrare lo step/tasklet e la transazionalità
- **checkpoint** persistente su tabella `job_schedules`
- **guardrails** (maxDuration attivo) e retry su ADX quando il resultset supera i limiti

> Nota: l’app è già abilitata a Batch e Scheduling tramite annotazioni in `IngestorApplicationApp`. citeturn119search96

---

## 1) Come usare questi prompt con GitHub Copilot in IntelliJ

### 1.1 Regole d’oro (per evitare “request too large” e codice incoerente)

1. **Esegui 1 prompt per volta**: non concatenare prompt diversi nello stesso messaggio.
2. **Non incollare allegati lunghi** (ERD/sequence/liquibase): i prompt sono già progettati per essere autosufficienti.
3. **Chiedi output “solo codice”** quando serve: nei prompt è già specificato; se Copilot aggiunge troppo testo, aggiungi in coda: _“Output: solo codice, senza spiegazioni”_.
4. **Verifica compilazione dopo ogni prompt** (build/test) e committa in step piccoli.
5. **Quando rilanci un prompt**, specifica sempre “aggiorna il codice esistente” (es. _“se le classi esistono già, modificale senza duplicarle”_).

### 1.2 Istruzioni da mettere nelle “Custom Instructions” di Copilot (consigliato)

Copia/incolla nelle istruzioni globali di Copilot (o in un file di istruzioni del progetto se lo usi):

- Genera **solo codice Java** compilabile e coerente con Spring Boot 3 / Java 17.
- **Non creare alternative parallele** (es. non usare `@Scheduled` se il prompt richiede Quartz).
- Se una classe/config esiste già, **aggiornala** invece di crearne una duplicata.
- Mantieni lo stile del progetto: QuartzJobBean lancia Job Spring Batch con JobLauncher e JobParameters. citeturn118search94turn118search93
- Ogni log deve includere `runId` e `entityName`.
- Quando definisci nuove proprietà, usa `@ConfigurationProperties(prefix="ingestion")`.

### 1.3 Come “eseguire” un prompt (passi pratici)

1. Apri **Copilot Chat** in IntelliJ.
2. Apri il file prompt (es. `02.scheduler_quartz_jobs.txt`) e **copia** il contenuto.
3. Incollalo in Copilot Chat e invia.
4. Applica le modifiche:
   - Se Copilot propone patch parziali, applica prima le classi “centrali” (config/job), poi i dettagli.
5. Esegui:
   - `mvn -q -DskipTests=false test` oppure il comando build standard del progetto.
6. Commit con messaggio del tipo: `ingestion: add quartz jobs skeleton`.

---

## 2) Ordine consigliato dei prompt (per minimizzare rework)

Esegui i prompt in questo ordine:

1. `01.init.txt`
2. `04.checkpoint_store.txt`
3. `05.end_limit_resolver.txt`
4. `06.guardrails.txt`
5. `07.adx_query_service.txt`
6. `08.generic_ingestion_runner.txt`
7. `03.batch_jobs_per_entity.txt`
8. `02.scheduler_quartz_jobs.txt`
9. `09.staging_and_reconciliation.txt`

Motivazione: costruisci prima i servizi core (checkpoint/endLimit/guardrails/ADX/runner), poi agganci Batch e infine Quartz.

---

## 3) Checklist di verifica dopo ogni prompt

### Compilazione e wiring Spring
- Nessun bean duplicato (Job/Step/QuartzConfiguration).
- Niente cicli di dipendenze tra bean.

### Quartz
- JobDetail e Trigger creati correttamente in `QuartzConfiguration` (tablePrefix `QRTZ_`, clustered=true già impostato). citeturn118search92turn118search89
- Ogni Quartz job usa `@DisallowConcurrentExecution`.
- Ogni Quartz job lancia il suo Job Batch con `JobLauncher.run(...)`. citeturn118search94turn118search93

### Spring Batch
- JobRepository configurato e prefisso `BATCH_` coerente. citeturn118search87turn118search88
- Ogni job batch ha 1 step tasklet che delega al runner.

### Logging
- Log START/END/ERROR sempre con `runId` e `entityName`.

---

## 4) Troubleshooting rapido

### “Oops, your request is too large”
- Riduci il prompt: esegui 1 file per volta.
- Evita di incollare ERD/sequence/liquibase completi.

### Copilot genera `@Scheduled` invece di Quartz
- Rilancia lo stesso prompt aggiungendo in cima:
  _“IMPORTANTE: non usare @Scheduled Spring, usare QuartzJobBean + QuartzConfiguration”_.

### Batch/Quartz non trovano le tabelle
- Verifica coerenza tra:
  - `spring.jpa.properties.hibernate.default_schema` e `spring.liquibase.default-schema` (ingestor)
  - proprietà `batch.jdbc.table-prefix` e `quartz.properties.org.quartz.jobStore.tablePrefix` in `application.yml` citeturn119search95
  - liquibase schema/prefix effettivi per `BATCH_` e `QRTZ_` citeturn118search88turn118search89

---

## 5) Nota su approccio “EntityName via JobParameters” vs “bean per entità”

Nel nostro playbook usiamo **bean per entità** (5 runner distinti + 5 job batch distinti). È più chiaro, più osservabile e più semplice da ottimizzare per entità (endLimit diversi, tuning diversi). È anche coerente con l’idea di 5 job schedulati. citeturn118search91

---

## 6) File di prompt

- `01.init.txt`
- `02.scheduler_quartz_jobs.txt`
- `03.batch_jobs_per_entity.txt`
- `04.checkpoint_store.txt`
- `05.end_limit_resolver.txt`
- `06.guardrails.txt`
- `07.adx_query_service.txt`
- `08.generic_ingestion_runner.txt`
- `09.staging_and_reconciliation.txt`

