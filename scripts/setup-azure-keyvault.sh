#!/bin/bash
# Script per setup Azure Key Vault e secret per tutti gli ambienti (dev/uat/prod)
# PREREQUISITO: az cli installato e connesso ad Azure

set -e

# ================================================================================
# CONFIGURAZIONE
# ================================================================================

TENANT_ID="7788edaf-0346-4068-9d79-c868aed15b3d"

# Ambienti e corrispondenti Resource Group + Vault nami
# Adatta i nomi e RG se differenti nel tuo Azure
declare -A ENVS_RG=(
    [dev]="pagopa-d-itn-crusc8"
    [uat]="pagopa-u-itn-crusc8"
    [prod]="pagopa-p-itn-crusc8"
)

declare -A ENVS_KV=(
    [dev]="pagopa-d-itn-crusc8-kv"
    [uat]="pagopa-u-itn-crusc8-kv"
    [prod]="pagopa-p-itn-crusc8-kv"
)

# Secret values (RIMPIAZZARE CON I TUOI VALORI REALI)
DB_PASSWORD_DEV="qualification_dashboard"
DB_PASSWORD_UAT="<uat-db-password>"
DB_PASSWORD_PROD="<prod-db-password>"

ADX_APP_ID_DEV="8677a3ca-8d05-453f-947c-0bdb1d15e512"
ADX_APP_ID_UAT="<uat-adx-app-id>"
ADX_APP_ID_PROD="<prod-adx-app-id>"

ADX_APP_KEY_DEV="qKa8Q~XkMUM_h4C1yzzGKt7Ao4jJ5Vft.rgJ1b1W"
ADX_APP_KEY_UAT="<uat-adx-app-key>"
ADX_APP_KEY_PROD="<prod-adx-app-key>"

declare -A DB_PASSWORDS=(
    [dev]="$DB_PASSWORD_DEV"
    [uat]="$DB_PASSWORD_UAT"
    [prod]="$DB_PASSWORD_PROD"
)

declare -A ADX_IDS=(
    [dev]="$ADX_APP_ID_DEV"
    [uat]="$ADX_APP_ID_UAT"
    [prod]="$ADX_APP_ID_PROD"
)

declare -A ADX_KEYS=(
    [dev]="$ADX_APP_KEY_DEV"
    [uat]="$ADX_APP_KEY_UAT"
    [prod]="$ADX_APP_KEY_PROD"
)

# ================================================================================
# FUNZIONI
# ================================================================================

create_or_update_secret() {
    local vault_name=$1
    local secret_name=$2
    local secret_value=$3

    echo "  → Setting secret '$secret_name' in vault '$vault_name'..."

    if [ -z "$secret_value" ] || [ "$secret_value" = "<${secret_name}-placeholder>" ]; then
        echo "    ⚠️  SKIPPED: secret value is placeholder. Update the script with real values."
        return
    fi

    az keyvault secret set \
        --vault-name "$vault_name" \
        --name "$secret_name" \
        --value "$secret_value" \
        2>/dev/null || {
        echo "    ❌ FAILED to set secret. Ensure vault exists and you have permission."
        return 1
    }

    echo "    ✅ Done"
}

create_keyvault_if_needed() {
    local rg=$1
    local vault_name=$2

    echo "Checking Key Vault: $vault_name (RG: $rg)..."

    if az keyvault show --name "$vault_name" --resource-group "$rg" &>/dev/null; then
        echo "  ✅ Vault '$vault_name' already exists"
    else
        echo "  → Creating vault '$vault_name'..."
        az keyvault create \
            --name "$vault_name" \
            --resource-group "$rg" \
            --enable-soft-delete true \
            --enable-purge-protection false \
            2>/dev/null || {
            echo "    ⚠️  Could not create vault. Ensure RG exists and you have permission."
            return 1
        }
        echo "    ✅ Vault created"
    fi
}

# ================================================================================
# MAIN
# ================================================================================

echo "========================================================================"
echo "Cruscotto Ingestor – Azure Key Vault Setup"
echo "========================================================================"
echo ""

# Verificare az cli connected
echo "Verifying Azure CLI connection..."
az account show >/dev/null || {
    echo "❌ Not connected to Azure. Run: az login"
    exit 1
}

echo "✅ Connected"
echo ""

# Per ogni ambiente
for env in dev uat prod; do
    rg="${ENVS_RG[$env]}"
    kv="${ENVS_KV[$env]}"
    db_pwd="${DB_PASSWORDS[$env]}"
    adx_id="${ADX_IDS[$env]}"
    adx_key="${ADX_KEYS[$env]}"

    echo "========================================================================"
    echo "Environment: $env"
    echo "========================================================================"

    # Creare vault se necessario
    if ! create_keyvault_if_needed "$rg" "$kv"; then
        echo "⚠️  Skipping $env environment"
        echo ""
        continue
    fi

    echo "Setting secrets in vault '$kv'..."
    create_or_update_secret "$kv" "db-cruscotto-password" "$db_pwd"
    create_or_update_secret "$kv" "adx-app-id" "$adx_id"
    create_or_update_secret "$kv" "adx-app-key" "$adx_key"

    echo ""
done

echo "========================================================================"
echo "Setup Complete!"
echo "========================================================================"
echo ""
echo "Next steps:"
echo "1. Verify all secrets are in place:"
echo "   az keyvault secret list --vault-name pagopa-d-itn-crusc8-kv"
echo ""
echo "2. For GitHub Actions:"
echo "   - Create Service Principal with OIDC federated credentials"
echo "   - Assign 'Key Vault Secrets User' role to each vault"
echo "   - Add GitHub secrets: AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID"
echo ""
echo "3. For Kubernetes (Workload Identity):"
echo "   - Service Account 'crusc8-workload-identity' must have:"
echo "     Role: 'Key Vault Secrets User'"
echo "     Scope: /subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.KeyVault/vaults/<VAULT_NAME>"
echo ""
echo "========================================================================"

