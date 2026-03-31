@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "APP_JAR=%SCRIPT_DIR%out\inme-hl7-monitor-client-1.0.0-SNAPSHOT.jar"
if not exist "%APP_JAR%" (
    set "APP_JAR=%SCRIPT_DIR%out\inme-hl7-monitor-client-1.0.0-SNAPSHOT-shaded.jar"
)
set "APP_MAIN_CLASS=com.zhunyintech.inmehl7.client.InmeHl7ClientApplication"
set "JAVAFX_VERSION=17.0.10"
set "M2_REPO=%USERPROFILE%\.m2\repository\org\openjfx"
set "JAVAFX_BASE=%M2_REPO%\javafx-base\%JAVAFX_VERSION%\javafx-base-%JAVAFX_VERSION%-win.jar"
set "JAVAFX_GRAPHICS=%M2_REPO%\javafx-graphics\%JAVAFX_VERSION%\javafx-graphics-%JAVAFX_VERSION%-win.jar"
set "JAVAFX_CONTROLS=%M2_REPO%\javafx-controls\%JAVAFX_VERSION%\javafx-controls-%JAVAFX_VERSION%-win.jar"
set "JAVAFX_MODULE_PATH=%JAVAFX_BASE%;%JAVAFX_GRAPHICS%;%JAVAFX_CONTROLS%"
set "JAVA_EXE="

if exist "C:\jdk-17.0.2\bin\java.exe" (
    set "JAVA_EXE=C:\jdk-17.0.2\bin\java.exe"
)

if not defined JAVA_EXE if defined JAVA_HOME (
    if exist "%JAVA_HOME%\bin\java.exe" (
        set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
    )
)

if not defined JAVA_EXE (
    set "JAVA_EXE=java"
)

if not exist "%APP_JAR%" (
    echo [ERROR] Jar not found: %APP_JAR%
    echo Build first with: mvn -DskipTests package
    pause
    exit /b 1
)

if not exist "%JAVAFX_BASE%" (
    echo [ERROR] JavaFX base jar not found: %JAVAFX_BASE%
    echo Run once: mvn dependency:go-offline
    pause
    exit /b 1
)

if not exist "%JAVAFX_GRAPHICS%" (
    echo [ERROR] JavaFX graphics jar not found: %JAVAFX_GRAPHICS%
    echo Run once: mvn dependency:go-offline
    pause
    exit /b 1
)

if not exist "%JAVAFX_CONTROLS%" (
    echo [ERROR] JavaFX controls jar not found: %JAVAFX_CONTROLS%
    echo Run once: mvn dependency:go-offline
    pause
    exit /b 1
)

cd /d "%SCRIPT_DIR%"
echo [INFO] Using Java: %JAVA_EXE%
echo [INFO] Using JavaFX module path: %JAVAFX_MODULE_PATH%
echo [INFO] Starting: %APP_MAIN_CLASS%
"%JAVA_EXE%" --module-path "%JAVAFX_MODULE_PATH%" --add-modules javafx.controls -Dfile.encoding=UTF-8 -cp "%APP_JAR%" %APP_MAIN_CLASS%
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo [ERROR] Client exited with code %EXIT_CODE%
    pause
)

exit /b %EXIT_CODE%
