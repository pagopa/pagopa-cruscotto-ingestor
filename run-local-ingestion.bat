@echo off
REM Local ingestion runner for cruscotto-ingestor
REM This batch file launches the PowerShell script with all required parameters

setlocal enabledelayedexpansion

cd /d "C:\Users\marco.colosi\git-repo\cruscotto\cruscotto-ingestor"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "run-local-ingestion.ps1" ^
  -DatasourceUrl "jdbc:postgresql://localhost:5432/cruscotto" ^
  -DatasourceUsername "cruscotto" ^
  -DatasourcePassword "qualification_dashboard" ^
  -SchemaName "sert_ingestor" ^
  -KustoClusterUrl "https://pagopaddataexplorer.westeurope.kusto.windows.net/" ^
  -KustoDatabaseName "re" ^
  -KustoAppId "8677a3ca-8d05-453f-947c-0bdb1d15e512" ^
  -KustoAppKey "qKa8Q~XkMUM_h4C1yzzGKt7Ao4jJ5Vft.rgJ1b1W" ^
  -KustoTenantId "7788edaf-0346-4068-9d79-c868aed15b3d" ^
  -ServerPort "8080"

pause

