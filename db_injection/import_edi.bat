@echo off
setlocal
set SCRIPT_DIR=%~dp0
set EXE=%SCRIPT_DIR%run_commands.exe

if not exist "%EXE%" (
	echo run_commands.exe not found in %SCRIPT_DIR%
	exit /b 1
)

set BASE_DIR=%SCRIPT_DIR%edi
if not exist "%BASE_DIR%" (
	echo EDI base directory not found: %BASE_DIR%
	exit /b 1
)

"%EXE%" import_edi_csv --base-dir "%BASE_DIR%"
endlocal
