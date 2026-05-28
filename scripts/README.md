# Setup Scripts – Cruscotto Ingestor

## Overview

Script per automatizzare il setup di Azure Key Vault e GitHub Secrets.

## Scripts

### `setup-azure-keyvault.ps1` (Windows / PowerShell)

**Prerequisito**: Azure CLI installed + logged in

```powershell
# Setup tutte gli ambienti (dev/uat/prod)
.\setup-azure-keyvault.ps1 -Environment all

# Setup solo dev
.\setup-azure-keyvault.ps1 -Environment dev
```

**Cosa fa:**
1. Verifica connessione az cli
2. Crea Key Vault per dev/uat/prod (se non esiste)
3. Popola i 3 secret per ogni vault:
   - `db-cruscotto-password`
   - `adx-app-id`
   - `adx-app-key`

**⚠️ IMPORTANTE:**
- Apri il file e **sostituisci i valori placeholder** (`<UPDATE-WITH-...>`) con i veri valori per uat/prod
- Per dev hai i valori già inseriti (adatta se necessario)

---

### `setup-azure-keyvault.sh` (macOS / Linux / WSL)

**Prerequisito**: Azure CLI installed + logged in

```bash
chmod +x setup-azure-keyvault.sh
./setup-azure-keyvault.sh
```

Stesso comportamento del PowerShell.

---

## Passo-Passo: Setup Completo

### 1. Login ad Azure

```powershell
az login
```

### 2. Edita i Valori dei Secret

Apri `setup-azure-keyvault.ps1` e aggiorna i placeholder:

```powershell
$Environments = @{
    uat = @{
        DbPassword      = "<UAT_DB_PASSWORD_REALE>"      # ← Rimpiazza qui
        AdxAppId        = "<UAT_ADX_APP_ID_REALE>"       # ← Rimpiazza qui
        AdxAppKey       = "<UAT_ADX_APP_KEY_REALE>"      # ← Rimpiazza qui
    }
    prod = @{
        DbPassword      = "<PROD_DB_PASSWORD_REALE>"
        AdxAppId        = "<PROD_ADX_APP_ID_REALE>"
        AdxAppKey       = "<PROD_ADX_APP_KEY_REALE>"
    }
}
```

### 3. Esegui lo Script

```powershell
cd C:\Users\marco.colosi\git-repo\cruscotto\cruscotto-ingestor
.\scripts\setup-azure-keyvault.ps1 -Environment all
```

### 4. Verifica i Secret

```powershell
# Vedi tutti i secret in dev vault
az keyvault secret list --vault-name pagopa-d-itn-crusc8-kv

# Leggi un secret specifico (per verificare)
az keyvault secret show --vault-name pagopa-d-itn-crusc8-kv --name db-cruscotto-password --query value
```

---

## Dopo il Setup: Permessi (IAM)

### Per Kubernetes (Workload Identity)

Ogni Service Account deve avere accesso al suo vault:

```bash
# Sostituisci <PRINCIPALID> e <VAULTSCOPE>
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee-object-id "<PRINCIPAL_ID>" \
  --assignee-principal-type ServicePrincipal \
  --scope "<VAULT_SCOPE>"
```

Esempio:
```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee-object-id "12345678-1234-1234-1234-123456789012" \
  --assignee-principal-type ServicePrincipal \
  --scope "/subscriptions/sub-id/resourceGroups/pagopa-d-itn-crusc8/providers/Microsoft.KeyVault/vaults/pagopa-d-itn-crusc8-kv"
```

### Per GitHub Actions (OIDC Federated)

Il Service Principal che usi in GitHub deve avere:

```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee-object-id "<GITHUB_SERVICE_PRINCIPAL_ID>" \
  --assignee-principal-type ServicePrincipal \
  --scope "<VAULT_SCOPE>"
```

---

## Troubleshooting

### "Permission denied" creation vault
- Assicurati che hai `Contributor` role nel Resource Group
- Verifica di aver fatto `az login` correttamente

### "Resource Group not found"
- Controlla che il Resource Group esista: `az group list --query "[].name"`
- Crea il RG manualmente se necessario:
  ```powershell
  az group create --name "pagopa-d-itn-crusc8" --location "westeurope"
  ```

### "Secret already exists"
- Lo script la aggiorna automaticamente. Se vuoi skip, rimuovi il secret manualmente:
  ```bash
  az keyvault secret delete --vault-name "..." --name "..." --no-wait
  ```

---

## Flusso Completo (Recap)

```
1. .\setup-azure-keyvault.ps1 -Environment all
         ↓
2. Verifica secret: az keyvault secret list --vault-name ...
         ↓
3. Assegna permessi IAM (Kubernetes SA, GitHub SP)
         ↓
4. GitHub Actions workflow legge automaticamente da Key Vault
         ↓
5. Helm deployment inietta i secret nei pod
```

---

## Reference

- Secret Management: `.github/SECRET_MANAGEMENT.md`
- GitHub Actions Setup: `.github/GITHUB_ACTIONS_SETUP.md`
- Helm Values: `helm/values-{dev,uat,prod}.yaml`

