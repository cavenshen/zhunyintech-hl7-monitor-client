Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$clientDir = Split-Path -Parent $scriptDir
$pomPath = Join-Path $clientDir 'pom.xml'

Write-Host "[1/1] Packaging inme-hl7-monitor-client" -ForegroundColor Cyan
mvn -f $pomPath -U -DskipTests clean package

