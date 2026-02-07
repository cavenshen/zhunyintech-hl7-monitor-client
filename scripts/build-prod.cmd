@echo off
setlocal

set SCRIPT_DIR=%~dp0
set CLIENT_DIR=%SCRIPT_DIR%..

echo [1/1] Packaging inme-hl7-monitor-client
mvn -f "%CLIENT_DIR%\pom.xml" -U -DskipTests clean package
if errorlevel 1 (
  echo Build failed
  exit /b 1
)
echo Build done

