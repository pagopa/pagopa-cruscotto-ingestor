# Secret Management – Architettura Helm + Azure Key Vault

## Concetto Fondamentale

**Non mettere mai secret nei file YAML.** Invece:

```
File YAML di config
    ↓ (contiene solo PLACEHOLDER)
    ↓
Azure Key Vault (contiene il vero valore)
    ↓ (Kubernetes legge il secret da qui)
    ↓
Pod (riceve il valore runtime via env var)
```

---

## Come Funziona Oggi (Helm + Azure)

Guarda il tuo `helm/values-dev.yaml` (linee 78-83):

```yaml
envSecret:
  SPRING_DATASOURCE_PASSWORD: "db-cruscotto-password"
  AZURE_KUSTO_APP_ID: "adx-app-id"
  AZURE_KUSTO_APP_KEY: "adx-app-key"
```

Questo dice: **"Leggi il valore dalla Key Vault, non da qui"**

Il nome `"db-cruscotto-password"` è un **PLACEHOLDER** → è il nome del secret in Azure Key Vault.

---

## I Tre Ambienti

### DEV
- **Key Vault**: `pagopa-d-itn-crusc8-kv` (linea 83 del values-dev.yaml)
- **Secret names** (dentro il vault):
  - `db-cruscotto-password` → password DB dev
  - `adx-app-id` → Service Principal ID dev
  - `adx-app-key` → Service Principal Secret dev

### UAT
- **Key Vault**: `pagopa-u-itn-crusc8-kv` (ipotesi: simile a dev)
- **Secret names**: uguali, ma valori diversi

### PROD
- **Key Vault**: `pagopa-p-itn-crusc8-kv` (ipotesi: simile a dev)
- **Secret names**: uguali, ma valori diversi

---

## Cosa Succede al Deploy

```
1. Developer fa: git push → develop
            ↓
2. GitHub Actions:
   - Autentica con Azure AD (Federated Credentials)
   - Chiede i secret dalla Key Vault
   - Passa a Maven via env var (compile time, non nei file)
   - Crea Docker image
            ↓
3. Helm deploy:
   - Legge secrets-ref dal values-dev.yaml
   - Chiede i valori veri dalla Key Vault
   - Inietta come env var nel Pod
            ↓
4. Pod (Kubernetes):
   - Riceve SPRING_DATASOURCE_PASSWORD da env
   - La app legge da env var, non da file
```

---

## Cosa Devi Creare su Azure

### Key Vault (uno per ambiente)

**DEV: `pagopa-d-itn-crusc8-kv`** (già hai il nome da Helm)

Dentro questo vault crei 3 secret:

| Secret Name | Tipo | Valore |
|-------------|------|--------|
| `db-cruscotto-password` | String | `qualification_dashboard` |
| `adx-app-id` | String | `8677a3ca-8d05-453f-947c-0bdb1d15e512` |
| `adx-app-key` | String | `qKa8Q~XkMUM_h4C1yzzGKt7Ao4jJ5Vft.rgJ1b1W` |

**Come crearlo** (Azure CLI):

```bash
# Login to Azure
az login

# Creare il vault (se non esiste)
az keyvault create --resource-group "pagopa-d-itn-crusc8" \
  --name "pagopa-d-itn-crusc8-kv"

# Aggiungere i secret
az keyvault secret set \
  --vault-name "pagopa-d-itn-crusc8-kv" \
  --name "db-cruscotto-password" \
  --value "qualification_dashboard"

az keyvault secret set \
  --vault-name "pagopa-d-itn-crusc8-kv" \
  --name "adx-app-id" \
  --value "8677a3ca-8d05-453f-947c-0bdb1d15e512"

az keyvault secret set \
  --vault-name "pagopa-d-itn-crusc8-kv" \
  --name "adx-app-key" \
  --value "qKa8Q~XkMUM_h4C1yzzGKt7Ao4jJ5Vft.rgJ1b1W"
```

oppure da Azure Portal (GUI):
1. **Vault** → `pagopa-d-itn-crusc8-kv`
2. **Secrets** → **+ Generate/Import**
3. Inserisci i 3 secret sopra

---

## File di Config (YAML) – Cosa C'è e Cosa NO

### ✅ DEV: `helm/values-dev.yaml`

```yaml
envSecret:
  SPRING_DATASOURCE_PASSWORD: "db-cruscotto-password"  # ← PLACEHOLDER (il vero valore è in Key Vault)
  AZURE_KUSTO_APP_ID: "adx-app-id"                     # ← PLACEHOLDER
  AZURE_KUSTO_APP_KEY: "adx-app-key"                   # ← PLACEHOLDER

keyvault:
  name: "pagopa-d-itn-crusc8-kv"  # ← Nome del vault dove cercate i secret
  tenantId: "7788edaf-0346-4068-9d79-c868aed15b3d"
```

### ❌ NON fare così:

```yaml
# VIETATO — non mettere il valore direttamente
envSecret:
  SPRING_DATASOURCE_PASSWORD: "qualification_dashboard"  # ❌ NO!
  AZURE_KUSTO_APP_KEY: "qKa8Q~XkMUM_..."                # ❌ NO!
```

---

## Flusso per Ambiente

### DEV (Kubernetes auto-inject via Workload Identity)

```
Helm chart deployment:
  ├─ Legge: envSecret.SPRING_DATASOURCE_PASSWORD: "db-cruscotto-password"
  ├─ Chiede a Key Vault: dammi il secret "db-cruscotto-password"
  │   (Workload Identity autentica automaticamente il pod)
  └─ Inietta nel pod env: SPRING_DATASOURCE_PASSWORD=<valore letto>
```

**Prerequisito**: Service Account del pod ha **Workload Identity** configurata → può leggere dal vault.

### GitHub Actions (Pull secret per build)

```
Workflow 05_build_dev.yml:
  ├─ Autentica con Azure AD (OIDC federated credentials)
  ├─ Chiede secrets dalla Key Vault
  └─ Passa a Maven:
       -D SPRING_DATASOURCE_PASSWORD=<secret>
```

### Local Development (Manuale)

Tu passi i valori via script:

```powershell
$env:SPRING_DATASOURCE_PASSWORD = "qualification_dashboard"
$env:AZURE_KUSTO_APP_ID = "8677a3ca-..."
$env:AZURE_KUSTO_APP_KEY = "qKa8Q~..."

.\run-local-ingestion.bat
```

**Oppure** leggi direttamente da Key Vault (Azure CLI):

```powershell
$secret = az keyvault secret show \
  --vault-name "pagopa-d-itn-crusc8-kv" \
  --name "db-cruscotto-password" \
  --query value -o tsv

$env:SPRING_DATASOURCE_PASSWORD = $secret
```

---

## Permessi Necessari (IAM)

### Per Kubernetes Pod (Workload Identity)

Service Account `crusc8-workload-identity` deve avere:
- **Role**: `Key Vault Secrets User`
- **Scope**: `pagopa-d-itn-crusc8-kv`

### Per GitHub Actions

Service Principal (used by GitHub OIDC) deve avere:
- **Role**: `Key Vault Secrets User`
- **Scope**: `pagopa-d-itn-crusc8-kv`

```bash
# Assegnare il role (Azure CLI)
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "<SERVICE_PRINCIPAL_CLIENT_ID>" \
  --scope "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RG>/providers/Microsoft.KeyVault/vaults/pagopa-d-itn-crusc8-kv"
```

---

## Checklist Setup

### Azure (una volta per ambiente)

- [ ] **UAT**: Creare vault `pagopa-u-itn-crusc8-kv` (o nome configurato)
- [ ] **PROD**: Creare vault `pagopa-p-itn-crusc8-kv` (o nome configurato)
- [ ] Per ogni vault: inserire i 3 secret (`db-cruscotto-password`, `adx-app-id`, `adx-app-key`)
- [ ] Assegnare role `Key Vault Secrets User` al Service Account Kubernetes (dev/uat/prod)
- [ ] Assegnare role `Key Vault Secrets User` al GitHub Actions Service Principal

### GitHub (una volta per repo)

- [ ] Creare GitHub environment `dev`, `uat`, `prod`
  - Variabili: `K8S_NAMESPACE`, `KEYVAULT_NAME`
  - Protection rules: Manual approval per prod
- [ ] Aggiungi GitHub Secrets:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
  - `KUBE_CONFIG` (base64)

### Helm (già fatto, solo verificare)

- [ ] `helm/values-dev.yaml` → `keyvault.name` corretto ✅
- [ ] `helm/values-uat.yaml` → `keyvault.name` corretto
- [ ] `helm/values-prod.yaml` → `keyvault.name` corretto

---

## Domande Frequenti

**Q: Posso leggere il secret dal vault in Spring?**
A: Sì, Spring Cloud Azure supporta Key Vault secret store, ma nella tua architectura meglio che Helm lo faccia (è più sicuro).

**Q: Se sbaglio il nome del secret nel values.yaml?**
A: Il pod non parte → errore "Secret not found in Key Vault". Controlli i log del deployment.

**Q: I secret scadono?**
A: Le password DB e ADX app key possono scadere. Devi aggiornarle periodicamente in Key Vault (l'app non ha bisogno di riavvio, prende il nuovo valore quando rilegge).

**Q: Come cambio un secret?**
A: Da Azure Portal o CLI:
```bash
az keyvault secret set --vault-name "pagopa-d-itn-crusc8-kv" \
  --name "db-cruscotto-password" --value "new-password"
```
Il pod lo legge "auto-magicamente" al prossimo accesso (cache dipende da kuberentes).

---

## Prossimi Step

1. Verificare il nome dei vault per uat/prod (posso controllare il tuo repo se vuoi)
2. Creare i vault se non esistono
3. Inserire i 3 secret in DEV vault (al minimo per testare)
4. Testare il deploy locale con env var
5. Testare GitHub Actions pull dei secret

Vuoi che ti prepari gli script Azure CLI per creare tutto in automatico?

