# Event Mosaic

Event Mosaic - приложение для загрузки, обработки и отображения событий GDELT на карте.

## Стек

- Java 25
- Spring Boot 4.1.0
- Gradle 9.5.1
- PostgreSQL
- Flyway
- Elasticsearch
- React 19
- TypeScript 6
- Vite 8

## Инфраструктура

Локально:

```shell
docker compose -f compose.local.yml up -d
```

## Локальный frontend

Frontend требует Node.js `24.19.0` и npm `11.19.0`.

Для совместной проверки карты с настоящим backend на Windows:

1. Подготовьте отдельные PostgreSQL и Elasticsearch:

   ```powershell
   .\tools\map-review\start-map-review.ps1
   ```

2. В IDEA выберите shared-конфигурацию `Event Mosaic Local` и нажмите `Run`.
3. После строки `Started EventMosaicApplication` откройте
   <http://127.0.0.1:5173>.
4. После проверки остановите конфигурацию в IDEA и удалите временный стек:

   ```powershell
   .\tools\map-review\stop-map-review.ps1
   ```

`.env` для этого сценария не нужен. Скрипты используют отдельные containers,
порты и volumes; назначение шагов описано в
[инструкции локального review](tools/map-review/README.md) и комментариях внутри
самих скриптов.

Если нужен только frontend, на Windows его можно открыть одним запуском из
корня проекта:

```powershell
.\run-frontend.cmd
```

Также можно дважды щелкнуть `run-frontend.cmd` в Проводнике. При наличии скрипт
сначала использует точный локальный runtime проекта; если его нет, проверяет
runtime из `PATH`. Затем он выполняет `npm ci`, запускает Vite только на
`127.0.0.1` и сам открывает браузер. Окно нужно оставить открытым, а для
остановки нажать `Ctrl+C`. Проверить bootstrap без запуска dev-сервера и
браузера можно командой `.\run-frontend.cmd --check`.

Чтобы карта получила проверенные границы стран и перешла в состояние
"Карта готова", сначала отдельно запустите backend конфигурацией Spring Boot в
IntelliJ IDEA или командой:

```powershell
# Windows
.\gradlew.bat bootRun
```

```shell
# macOS
sh ./gradlew bootRun
```

На macOS или для ручного запуска Vite установите зависимости только из lockfile
и откройте dev-сервер так:

```shell
cd frontend
npm ci
npm run dev -- --host 127.0.0.1 --open
```

Vite по умолчанию проксирует запросы `/map` и `/api` на
`http://localhost:8080`. Другой адрес backend можно задать в локальном
`frontend/.env.local` по образцу `frontend/.env.example`; этот файл не нужно
добавлять в Git. Если backend не запущен или вернул несовместимую геометрию,
frontend показывает отдельную ошибку и не выдает пустую карту за успешный
результат. Сейчас frontend собирается отдельно и пока не включается в Spring
Boot artifact.

## Геометрия карты

Опубликованную country-геометрию можно полностью проверить без исходных
Natural Earth данных и доступа к сети:

```shell
npm --prefix tools/map-geometry run geometry:verify -- --published
```

Подготовка новой версии, поддерживаемые команды Windows/macOS и безопасное
восстановление описаны в [инструкции по геометрии карты](tools/map-geometry/README.md).

## Эксплуатационные операции

- В приложении есть server-side one-shot команды, которые заново собирают
  поврежденный фрагмент поисковых данных или явно очищают одну technical
  generation по exact именам и UUID. Они предназначены только для аварийного
  break-glass доступа. После согласования production security и deployment в
  отдельном change должны появиться защищенный асинхронный endpoint и экран
  Operator как основной штатный интерфейс. [Инструкция для разработчика и
  администратора](docs/operations/partition-rebuild.md).
