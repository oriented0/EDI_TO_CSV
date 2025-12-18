@echo off
setlocal
set SCRIPT_DIR=%~dp0
set EXE=%SCRIPT_DIR%run_commands.exe

if not exist "%EXE%" (
	echo run_commands.exe not found in %SCRIPT_DIR%
	exit /b 1
)

set CSV_PATH=%SCRIPT_DIR%patient\patient.csv
if not exist "%CSV_PATH%" (
	echo patient.csv not found at %CSV_PATH%
	exit /b 1
)

"%EXE%" import_patients_from_csv "%CSV_PATH%"
endlocal
