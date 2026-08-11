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

На Windows ранний экран можно открыть одним запуском из корня проекта:

```powershell
.\run-frontend.cmd
```

Также можно дважды щелкнуть `run-frontend.cmd` в Проводнике. При наличии скрипт
сначала использует точный локальный runtime проекта; если его нет, проверяет
runtime из `PATH`. Затем он выполняет `npm ci`, запускает Vite только на
`127.0.0.1` и сам открывает браузер. Окно нужно оставить открытым, а для
остановки нажать `Ctrl+C`. Проверить bootstrap без запуска dev-сервера и
браузера можно командой `.\run-frontend.cmd --check`.

Текущий экран-каркас еще не обращается к backend, поэтому для его просмотра
Spring Boot не нужен. Когда потребуется проверить `/map`, backend можно
запустить вручную конфигурацией Spring Boot в IntelliJ IDEA или командой:

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

Vite по умолчанию проксирует только запросы `/map` на
`http://localhost:8080`. Другой адрес backend можно задать в локальном
`frontend/.env.local` по образцу `frontend/.env.example`; этот файл не нужно
добавлять в Git. Сейчас frontend собирается отдельно и пока не включается в
Spring Boot artifact.

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
