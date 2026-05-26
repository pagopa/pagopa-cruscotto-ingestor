param(
    [string]$Environment = "all",  # all, dev, uat, prod
    [switch]$Force
)

<#
.SYNOPSIS
    Setup Azure Key Vault secrets for cruscotto-ingestor (dev/uat/prod)

.DESCRIPTION
    Creates Key Vaults and populates them with ADX + DB secrets
    PREREQUISITO: Azure CLI installed and logged in (az login)

.EXAMPLE
    .\scripts\setup-azure-keyvault.ps1 -Environment dev
    .\scripts\setup-azure-keyvault.ps1 -Environment all
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ================================================================================
# CONFIGURATION
# ================================================================================

$TenantId = "7788edaf-0346-4068-9d79-c868aed15b3d"

$Environments = @{
    dev = @{
        ResourceGroup   = "pagopa-d-itn-crusc8"
        KeyVault        = "pagopa-d-itn-crusc8-kv"
        DbPassword      = "qualification_dashboard"
        AdxAppId        = "8677a3ca-8d05-453f-947c-0bdb1d15e512"
        AdxAppKey       = "qKa8Q~XkMUM_h4C1yzzGKt7Ao4jJ5Vft.rgJ1b1W"  # ⚠️ Change if real value differs
    }
    uat = @{
        ResourceGroup   = "pagopa-u-itn-crusc8"
        KeyVault        = "pagopa-u-itn-crusc8-kv"
        DbPassword      = "<UPDATE-WITH-UAT-PASSWORD>"
        AdxAppId        = "<UPDATE-WITH-UAT-ADX-APP-ID>"
        AdxAppKey       = "<UPDATE-WITH-UAT-ADX-APP-KEY>"
    }
    prod = @{
        ResourceGroup   = "pagopa-p-itn-crusc8"
        KeyVault        = "pagopa-p-itn-crusc8-kv"
        DbPassword      = "<UPDATE-WITH-PROD-PASSWORD>"
        AdxAppId        = "<UPDATE-WITH-PROD-ADX-APP-ID>"
        AdxAppKey       = "<UPDATE-WITH-PROD-ADX-APP-KEY>"
    }
}

# Secret names (same for all environments)
$SecretNames = @{
    DbPassword = "db-cruscotto-password"
    AdxAppId   = "adx-app-id"
    AdxAppKey  = "adx-app-key"
}

# ================================================================================
# FUNCTIONS
# ================================================================================

function Test-AzureCliConnected {
    try {
        $null = az account show --query id -o tsv 2>$null
        return $true
    } catch {
        return $false
    }
}

function Create-KeyVaultIfNeeded {
    param([string]$ResourceGroup, [string]$VaultName)

    Write-Host "Checking Key Vault: $VaultName (RG: $ResourceGroup)..." -ForegroundColor Cyan

    try {
        $vault = az keyvault show --name $VaultName --resource-group $ResourceGroup --query id -o tsv 2>$null
        if ($vault) {
            Write-Host "  ✅ Vault '$VaultName' already exists" -ForegroundColor Green
            return $true
        }
    } catch {
        # Vault doesn't exist, will create below
    }

    Write-Host "  → Creating vault '$VaultName'..." -ForegroundColor Yellow
    try {
        az keyvault create `
            --name $VaultName `
            --resource-group $ResourceGroup `
            --enable-soft-delete true `
            --enable-purge-protection false | Out-Null
        Write-Host "    ✅ Vault created" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "    ❌ Failed to create vault. Ensure RG exists and you have permission." -ForegroundColor Red
        return $false
    }
}

function Set-KeyVaultSecret {
    param(
        [string]$VaultName,
        [string]$SecretName,
        [string]$SecretValue
    )

    Write-Host "  → Setting secret '$SecretName' in vault '$VaultName'..." -ForegroundColor Yellow

    if ([string]::IsNullOrEmpty($SecretValue) -or $SecretValue -match "^<.*>$") {
        Write-Host "    ⚠️  SKIPPED: secret value is placeholder. Update config with real values." -ForegroundColor Yellow
        return
    }

    try {
        az keyvault secret set `
            --vault-name $VaultName `
            --name $SecretName `
            --value $SecretValue | Out-Null
        Write-Host "    ✅ Done" -ForegroundColor Green
    } catch {
        Write-Host "    ❌ Failed to set secret. Check vault exists and you have permission." -ForegroundColor Red
        throw $_
    }
}

# ================================================================================
# MAIN
# ================================================================================

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "Cruscotto Ingestor – Azure Key Vault Setup (PowerShell)" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host ""

# Check Azure CLI
Write-Host "Verifying Azure CLI connection..." -ForegroundColor Yellow
if (-not (Test-AzureCliConnected)) {
    Write-Host "❌ Not connected to Azure. Run: az login" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Connected" -ForegroundColor Green
Write-Host ""

# Determine which environments to process
$EnvsToProcess = @()
if ($Environment -eq "all") {
    $EnvsToProcess = $Environments.Keys | Sort-Object
} elseif ($Environments.ContainsKey($Environment)) {
    $EnvsToProcess = @($Environment)
} else {
    Write-Host "❌ Unknown environment: $Environment. Valid: all, dev, uat, prod" -ForegroundColor Red
    exit 1
}

# Process each environment
$SuccessCount = 0
foreach ($env in $EnvsToProcess) {
    $config = $Environments[$env]

    Write-Host "========================================================================" -ForegroundColor Cyan
    Write-Host "Environment: $env" -ForegroundColor Cyan
    Write-Host "========================================================================" -ForegroundColor Cyan

    # Create vault
    if (-not (Create-KeyVaultIfNeeded -ResourceGroup $config.ResourceGroup -VaultName $config.KeyVault)) {
        Write-Host "⚠️  Skipping $env environment" -ForegroundColor Yellow
        Write-Host ""
        continue
    }

    # Set secrets
    Write-Host "Setting secrets in vault '$($config.KeyVault)'..." -ForegroundColor Cyan
    try {
        Set-KeyVaultSecret -VaultName $config.KeyVault `
            -SecretName $SecretNames.DbPassword `
            -SecretValue $config.DbPassword

        Set-KeyVaultSecret -VaultName $config.KeyVault `
            -SecretName $SecretNames.AdxAppId `
            -SecretValue $config.AdxAppId

        Set-KeyVaultSecret -VaultName $config.KeyVault `
            -SecretName $SecretNames.AdxAppKey `
            -SecretValue $config.AdxAppKey

        $SuccessCount++
    } catch {
        Write-Host "⚠️  Some secrets failed in $env" -ForegroundColor Yellow
    }

    Write-Host ""
}

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "Setup Complete! (Successful: $SuccessCount/$($EnvsToProcess.Count))" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Verify secrets are in place:" -ForegroundColor White
Write-Host "   az keyvault secret list --vault-name pagopa-d-itn-crusc8-kv" -ForegroundColor Gray
Write-Host ""

Write-Host "2. For GitHub Actions:" -ForegroundColor White
Write-Host "   - Create Service Principal with OIDC federated credentials" -ForegroundColor Gray
Write-Host "   - Assign 'Key Vault Secrets User' role to each vault" -ForegroundColor Gray
Write-Host "   - Add GitHub secrets: AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID" -ForegroundColor Gray
Write-Host ""

Write-Host "3. For Kubernetes (Workload Identity):" -ForegroundColor White
Write-Host "   - Service Account 'crusc8-workload-identity' must have:" -ForegroundColor Gray
Write-Host "     Role: 'Key Vault Secrets User'" -ForegroundColor Gray
Write-Host "     Scope: /subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.KeyVault/vaults/<VAULT_NAME>" -ForegroundColor Gray
Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan

