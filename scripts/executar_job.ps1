<#
.SYNOPSIS
  Faz deploy do bundle e executa o job, exibindo so o essencial (sem ruido de ANSI/stack de notebook).
.EXAMPLE
  .\scripts\executar_job.ps1 -Target dev
  .\scripts\executar_job.ps1 -Target dev -DataCarga 2026-09-20 -Reprocessar true
  .\scripts\executar_job.ps1 -Target dev -Only preparar,bronze
#>
param(
    [ValidateSet("dev", "stg", "prod")][string]$Target = "dev",
    [string]$DataCarga = "auto",
    [string]$Reprocessar = "false",
    [string[]]$Only = @(),
    [switch]$SemDeploy
)

$ErrorActionPreference = "Continue"
Set-Location (Split-Path $PSScriptRoot -Parent)
$venv = Join-Path (Get-Location) ".venv\Scripts"
if (Test-Path $venv) { $env:PATH = "$venv;$env:PATH" }   # precisa do pacote 'build' no PATH

function Limpar([string]$texto) { $texto -replace "\x1B\[[0-9;]*m", "" }

if (-not $SemDeploy) {
    Write-Host ">> deploy ($Target)"
    databricks bundle deploy -t $Target 2>&1 | ForEach-Object { Limpar "$_" }
}

$args_run = @("bundle", "run", "marketing_pipeline", "-t", $Target, "--params", "data_carga=$DataCarga,reprocessar=$Reprocessar")
if ($Only.Count -gt 0) { $args_run += @("--only", ($Only -join ",")) }
Write-Host ">> run ($Target) data_carga=$DataCarga reprocessar=$Reprocessar"
databricks @args_run 2>&1 | ForEach-Object { Limpar "$_" } | Select-Object -First 60
