@echo off
rem Run the whole system without Docker on Windows. Sets itself up on first use.
rem Stop it with Ctrl+C. Written carefully but not yet run on a real Windows machine;
rem Docker (see compose.yaml) is the tested way.
setlocal
cd /d "%~dp0.."

if not exist ".venv\Scripts\polyweather.exe" (
  py -3 -m venv .venv 2>nul || python -m venv .venv || goto :nopython
  ".venv\Scripts\python.exe" -m pip install --quiet --disable-pip-version-check -e . || goto :failed
)

set PYTHONUTF8=1
if not defined POLYWEATHER_DATA_DIR set "POLYWEATHER_DATA_DIR=%CD%\data"
".venv\Scripts\polyweather.exe" run-all %*
exit /b %ERRORLEVEL%

:nopython
echo Python 3.11 or newer is needed: https://www.python.org/downloads/
exit /b 1

:failed
echo Installing the program failed. See the messages above.
exit /b 1
