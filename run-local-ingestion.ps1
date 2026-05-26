param(
    [string]$DatasourceUrl = "jdbc:postgresql://localhost:5432/cruscotto",
    [string]$DatasourceUsername = "cruscotto",
    [string]$DatasourcePassword = "",
    [string]$SchemaName = "sert_ingestor",
    [string]$KustoClusterUrl = "https://pagopaddataexplorer.westeurope.kusto.windows.net/",
    [string]$KustoDatabaseName = "re",
    [string]$KustoAppId = $env:AZURE_KUSTO_APP_ID,
    [string]$KustoAppKey = $env:AZURE_KUSTO_APP_KEY,
    [string]$KustoTenantId = $env:AZURE_KUSTO_TENANT_ID,
    [string]$ServerPort = "8080"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$missing = @()
if ([string]::IsNullOrWhiteSpace($KustoAppId)) { $missing += "AZURE_KUSTO_APP_ID" }
if ([string]::IsNullOrWhiteSpace($KustoAppKey)) { $missing += "AZURE_KUSTO_APP_KEY" }
if ([string]::IsNullOrWhiteSpace($KustoTenantId)) { $missing += "AZURE_KUSTO_TENANT_ID" }

if ($missing.Count -gt 0) {
    Write-Host "Missing required ADX variables for ingestion jobs:" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host " - $_" -ForegroundColor Red }
    Write-Host ""
    Write-Host "Example:" -ForegroundColor Yellow
    Write-Host '$env:AZURE_KUSTO_APP_ID="<app-id>"'
    Write-Host '$env:AZURE_KUSTO_APP_KEY="<app-key>"'
    Write-Host '$env:AZURE_KUSTO_TENANT_ID="<tenant-id>"'
    Write-Host '.\run-local-ingestion.ps1'
    exit 1
}

# Core runtime variables
$env:SPRING_DATASOURCE_URL = $DatasourceUrl
$env:SPRING_DATASOURCE_USERNAME = $DatasourceUsername
$env:SPRING_DATASOURCE_PASSWORD = $DatasourcePassword
$env:APP_DB_SCHEMA_NAME = $SchemaName
$env:SERVER_PORT = $ServerPort

# ADX variables
$env:AZURE_KUSTO_CLUSTER_URL = $KustoClusterUrl
$env:AZURE_KUSTO_DATABASE_NAME = $KustoDatabaseName
$env:AZURE_KUSTO_APP_ID = $KustoAppId
$env:AZURE_KUSTO_APP_KEY = $KustoAppKey
$env:AZURE_KUSTO_TENANT_ID = $KustoTenantId

# Note: ingestion.quartz.jobs.* is already fully configured in application.yml
# Partial env var overrides (only .enabled) cause Spring binding errors.
# Keep it clean: let application.yml define the complete job configs.

Push-Location $PSScriptRoot
try {
    Write-Host "Starting ingestor with Quartz jobs enabled..." -ForegroundColor Cyan
    Write-Host "DB URL: $DatasourceUrl"
    Write-Host "DB Schema: $SchemaName"
    Write-Host "ADX Cluster: $KustoClusterUrl"
    Write-Host "ADX Database: $KustoDatabaseName"

    mvn spring-boot:run
}
finally {
    Pop-Location
}

