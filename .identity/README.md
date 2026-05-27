# .identity/

Terraform infrastructure as code per gestire GitHub environments + secrets + Azure Key Vault integration.

## 📋 Struttura

```
.identity/
├── 00_data.tf              # Data sources (Key Vault, AKS, GitHub teams)
├── 01_locals.tf            # Local variables (cluster, postgres DB, etc.)
├── 03_github_environment.tf # GitHub environments, secrets injection
├── 99_main.tf              # Provider config (Azure, GitHub, OIDC)
├── 99_variables.tf         # Input variables
├── terraform.sh            # Bash launcher
└── env/
    ├── dev/
    │   ├── backend.ini
    │   ├── backend.tfvars
    │   └── terraform.tfvars
    ├── uat/
    │   ├── backend.ini
    │   ├── backend.tfvars
    │   └── terraform.tfvars
    └── prod/
        ├── backend.ini
        ├── backend.tfvars
        └── terraform.tfvars
```

## 🚀 Quick Start

### 1. Prerequisiti

```bash
# Terraform >= 1.3.0
terraform --version

# Azure CLI
az --version

# Git bash o WSL (per terraform.sh)
which bash
```

### 2. Autenticazione Azure

```bash
# Login
az login
az account show

# Seleziona la subscription corretta
az account set -s "<subscription-id>"
```

### 3. GitHub Token

Vai a GitHub → Settings → Developer settings → Personal access tokens (classic):
- Scopes: `repo`, `admin:org`
- Salva il token env var: `export GITHUB_TOKEN="ghp_..."`

### 4. Esegui Terraform

```bash
cd .identity

# DEV
bash terraform.sh plan dev
bash terraform.sh apply dev

# UAT
bash terraform.sh plan uat
bash terraform.sh apply uat

# PROD (richiede approval se protected)
bash terraform.sh plan prod
bash terraform.sh apply prod
```

## 📊 Cosa Fa

Per ogni environment (dev/uat/prod):

1. **GitHub Environment** creato con nome `dev`, `uat`, `prod`
2. **7 Secrets injectati** da Azure Key Vault:
   - `CD_CLIENT_ID` (Service Principal per deploy)
   - `TENANT_ID`, `SUBSCRIPTION_ID`
   - `POSTGRES_DB_PASSWORD` (da Key Vault)
   - `ADX_APP_ID`, `ADX_APP_KEY` (da Key Vault)
3. **9 Variabili injectati:**
   - Nomi cluster K8s
   - Namespace
   - Workload Identity ID
   - DB connection params

## ⚙️ Configurazione

Modifica i file in `env/{env}/backend.ini` se:
- Storage account name cambia
- Subscription ID cambia
- Resource group cambia

Modifica `env/{env}/terraform.tfvars` per:
- Reviewers teams (prod only)
- Protection rules

## ✅ Verifica

```bash
# GitHub environments creati?
gh api repos/pagopa/cruscotto-ingestor/environments \
  --jq '.[].name'

# Secrets injectati?
gh secret list --env dev --repo pagopa/cruscotto-ingestor

# Variables injectati?
gh variable list --env dev --repo pagopa/cruscotto-ingestor
```

## 📝 Note

- **Dev/UAT**: No approval needed
- **Prod**: Richiede manual approval (reviewer_teams configurati in Terraform)
- **State file**: Stored in Azure Storage Account (backend azurerm)
- **OIDC**: GitHub Actions autentica con Azure via federated credentials

## 🔗 References

- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest)
- [Terraform GitHub Provider](https://registry.terraform.io/providers/integrations/github/latest)
- [Azure OIDC + GitHub](https://learn.microsoft.com/en-us/azure/developer/github/connect-from-azure?tabs=azure-cli%2Clinux)

