@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul

set "EXPECTED_NODE=v24.19.0"
set "EXPECTED_NPM=11.19.0"
set "PROJECT_ROOT=%~dp0"
set "LOCAL_RUNTIME=%PROJECT_ROOT%.local\node-v24.19.0-win-x64"
set "CHECK_ONLY="
set "MODE=%~1"

if /I "%MODE%"=="--check" set "CHECK_ONLY=1"
if not "%~2"=="" goto :unknown_arguments
if "%MODE%"=="" goto :arguments_ready
if defined CHECK_ONLY goto :arguments_ready

:unknown_arguments
echo Переданы неизвестные аргументы.
echo Поддерживается только необязательный аргумент --check.
goto :failed

:arguments_ready
if exist "%LOCAL_RUNTIME%\node.exe" if exist "%LOCAL_RUNTIME%\npm.cmd" set "PATH=%LOCAL_RUNTIME%;%PATH%"

set "ACTUAL_NODE="
for /f "delims=" %%V in ('node --version 2^>nul') do set "ACTUAL_NODE=%%V"
if not defined ACTUAL_NODE (
  echo Не найден Node.js %EXPECTED_NODE%.
  echo Установите точную версию или подготовьте локальный runtime проекта.
  goto :failed
)

if not "%ACTUAL_NODE%"=="%EXPECTED_NODE%" (
  echo Нужен Node.js %EXPECTED_NODE%, найден %ACTUAL_NODE%.
  echo Установите точную версию или исправьте локальный runtime проекта.
  goto :failed
)

set "ACTUAL_NPM="
for /f "delims=" %%V in ('call npm --version 2^>nul') do set "ACTUAL_NPM=%%V"
if not defined ACTUAL_NPM (
  echo Не найден npm %EXPECTED_NPM%.
  goto :failed
)

if not "%ACTUAL_NPM%"=="%EXPECTED_NPM%" (
  echo Нужен npm %EXPECTED_NPM%, найден %ACTUAL_NPM%.
  echo Обновите npm в выбранном Node.js runtime до точной версии.
  goto :failed
)

if not exist "%PROJECT_ROOT%frontend\package.json" (
  echo Не найден frontend\package.json относительно %PROJECT_ROOT%
  goto :failed
)

if not exist "%PROJECT_ROOT%frontend\package-lock.json" (
  echo Не найден frontend\package-lock.json относительно %PROJECT_ROOT%
  goto :failed
)

echo Runtime: Node.js %ACTUAL_NODE%, npm %ACTUAL_NPM%
echo Устанавливаю frontend-зависимости только из lockfile...

pushd "%PROJECT_ROOT%frontend" >nul
if errorlevel 1 (
  echo Не удалось перейти в каталог frontend.
  goto :failed
)

call npm ci --no-audit --no-fund
if errorlevel 1 (
  popd
  echo Не удалось установить frontend-зависимости.
  goto :failed
)

if defined CHECK_ONLY goto :check_mode

echo.
echo Запускаю frontend на loopback-адресе и открываю браузер.
echo Для загрузки границ стран отдельно запустите Spring Boot backend.
echo Для остановки нажмите Ctrl+C в этом окне.
call npm run dev -- --host 127.0.0.1 --open
set "DEV_EXIT=%ERRORLEVEL%"
popd

if not "%DEV_EXIT%"=="0" (
  echo Vite завершился с кодом %DEV_EXIT%.
  goto :failed
)

exit /b 0

:check_mode
echo.
echo Проверяю, что установленный frontend собирается без запуска браузера...
call npm run build
set "BUILD_EXIT=%ERRORLEVEL%"
popd

if not "%BUILD_EXIT%"=="0" (
  echo Проверка frontend завершилась с кодом %BUILD_EXIT%.
  goto :failed
)

echo Launcher готов: runtime, npm ci и frontend build работают.
exit /b 0

:failed
if not defined CHECK_ONLY pause
exit /b 1
