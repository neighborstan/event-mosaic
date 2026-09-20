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

Для обычной работы с сохранением накопленных событий используйте [постоянный локальный запуск](docs/operations/local-ingestion.md). Остановка Spring/Vite и `docker compose -f compose.local.yml stop` сохраняет базу, поисковые данные и подготовленные файлы. Обычный запуск Spring автоматически получает свежие GDELT данные и восстанавливает суточное окно карты.

## Собранное приложение без Vite

### Обычный запуск из IntelliJ IDEA

Если контейнеры из `tools/map-review/start-map-review.ps1` уже работают, выберите **Event Mosaic Local** в списке конфигураций IDEA и нажмите **Run**. После строки `Started EventMosaicApplication` откройте [карту](http://127.0.0.1:18080/). Запоминать команды, менять PATH и отдельно запускать Vite не требуется.

Конфигурация запускает Gradle `bootRun`: он проверяет Node.js/npm, при необходимости устанавливает зависимости и собирает актуальные frontend-файлы, затем запускает Spring Boot. Карта и API доступны на одном адресе. После изменения frontend нажмите Stop и снова Run, чтобы увидеть новую сборку. В IDEA проект должен быть импортирован как Gradle-проект; если измененные конфигурации еще не появились, выполните Reload All Gradle Projects и заново выберите `Event Mosaic Local`.

Кнопка **Stop** останавливает приложение; контейнеры и накопленные данные сохраняются. Следующий Run использует те же PostgreSQL `15432`, Elasticsearch `19200` и `.local/gdelt`. Это прежний review-стек, его подключения не перенесены на другой набор данных. `stop-map-review.ps1` нужен только для явного удаления этого стека с данными. Для первого запуска контейнеров используйте [инструкцию подготовки](tools/map-review/README.md).

Конфигурация `Event Mosaic Backend` остается отдельным запуском Spring для разработки Java. Для гарантированной сборки frontend используйте `Event Mosaic Local`: она явно выполняет Gradle tasks и не зависит от выбора встроенного сборщика IDEA.

### Сборка и запуск JAR

Для сборки нужны JDK 25, Node.js `24.19.0`, npm `11.19.0` и работающий Docker для интеграционных тестов. На Windows Gradle автоматически использует подготовленный `.local/node-v24.19.0-win-x64`, включая npm из этого каталога. Если каталога нет, он использует `PATH`; на macOS также используется `PATH`. Ручная настройка PATH для уже подготовленного Windows-проекта не нужна. Неполный или несовместимый выбранный runtime останавливает сборку с объяснением; Gradle сам Node.js не скачивает и молча на другую версию не переключается. При первичной подготовке установите указанные версии; npm обновляется командой `npm install --global npm@11.19.0` в выбранном Node.js runtime.

Остановите Vite перед чистой установкой: на Windows работающий сервер может удерживать файлы `node_modules`. Затем соберите приложение из корня проекта:

```powershell
# Windows
.\gradlew.bat clean build
```

```shell
# macOS
sh ./gradlew clean build
```

Gradle выполняет `npm ci` из lockfile и Vite build, помещает HTML/JS/CSS и отдельный обработчик геометрии MapLibre в `build/generated-resources/frontend/static` и включает их вместе с геометрией в `build/libs/event-mosaic-0.1.0-SNAPSHOT.jar`. Исходные ресурсы не изменяются; повторная сборка учитывает frontend-файлы, локальные `.env*` и переменные `VITE_*`. Каталоги сборки и `node_modules` не нужно добавлять в Git.

Правила сборки frontend находятся в локальном плагине `buildSrc/src/main/kotlin/event-mosaic.frontend.gradle.kts`; основной `build.gradle.kts` подключает его через `id("event-mosaic.frontend")`. Обычные правки компонентов, стилей и npm-зависимостей не требуют изменения Gradle: обновите frontend-файлы и при необходимости lockfile, затем пересоберите JAR. Плагин меняется только при изменении способа сборки, путей или закрепленных версий Node.js/npm.

Поднимите инфраструктуру и запустите JAR на Windows:

```powershell
docker compose -f compose.local.yml up -d
$env:SPRING_DATASOURCE_URL = "jdbc:postgresql://localhost:5432/event_mosaic"
$env:SPRING_DATASOURCE_USERNAME = "event_mosaic"
$env:SPRING_DATASOURCE_PASSWORD = "event_mosaic"
$env:SPRING_ELASTICSEARCH_URIS = "http://localhost:9200"
java -jar build/libs/event-mosaic-0.1.0-SNAPSHOT.jar --server.address=127.0.0.1
```

На macOS:

```shell
docker compose -f compose.local.yml up -d
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/event_mosaic
export SPRING_DATASOURCE_USERNAME=event_mosaic
export SPRING_DATASOURCE_PASSWORD=event_mosaic
export SPRING_ELASTICSEARCH_URIS=http://localhost:9200
java -jar build/libs/event-mosaic-0.1.0-SNAPSHOT.jar --server.address=127.0.0.1
```

Откройте [карту](http://127.0.0.1:8080/). HTML, файлы сборки, `/map/geometry/country-v1/countries.geojson` и `/api/v1/map/country-snapshot` обслуживает один Spring Boot. Для запуска готового JAR Node.js/npm и Vite не нужны; JDK 25, доступные PostgreSQL/Elasticsearch, свободное место для staging и интернет для GDELT и фоновой карты нужны. Docker Compose интеграция режима разработки не входит в JAR, поэтому подключения заданы явно. Приведенные credentials предназначены только для локального Compose.

Обычный запуск сам получает GDELT; флаг one-shot не требуется. В начале карта может показывать отсутствие данных или частичное покрытие. Экран принимает обновление каждые 15 минут; для немедленной проверки уже загруженных данных обновите страницу. Для остановки нажмите `Ctrl+C`, затем выполните `docker compose -f compose.local.yml stop`. Повторите `up -d` и ту же команду `java -jar` из того же каталога: база, индексы и `.local/gdelt` сохраняются. Подробности и режим без загрузки описаны в [инструкции постоянного запуска](docs/operations/local-ingestion.md).

### Фоновая карта

По умолчанию используется OpenFreeMap без browser key. Для явного выбора задайте `VITE_MAP_BASEMAP_PROVIDER=openfreemap` или `stadia` в `frontend/.env.local` по образцу `.env.example` и пересоберите JAR. Это настройка времени сборки, переменная после запуска JAR карту не переключает. `MAP_BACKEND_ORIGIN` используется только proxy Vite и не меняет адреса API готового приложения.

Подписи источников в карте должны оставаться видимыми: OpenFreeMap/OpenMapTiles/OpenStreetMap либо Stadia Maps/OpenMapTiles/OpenStreetMap для используемого стиля `alidade_smooth`. Для Stadia на публичном адресе предварительно настройте account/property, тариф и разрешенный domain, проверьте `Origin`, `Referer` и `Referrer-Policy` по [официальной инструкции](https://docs.stadiamaps.com/authentication/). Работа на localhost не доказывает доступность публичного домена. Ключи и секреты в `VITE_*` и browser bundle не добавляются; автоматического переключения provider нет.

## Разработка с отдельным Vite

Карта показывает сводку за последние 24 часа по странам. В режиме "Тональность" ровная окраска различает преобладание отрицательных или положительных событий, смешанную картину и настоящий нулевой тон. В режиме "Количество событий" цвет показывает общее число событий. Выберите страну на карте или в списке, чтобы увидеть точные доли всех семи диапазонов; при увеличении карта пока сохраняет сводку по стране.

Frontend требует Node.js `24.19.0` и npm `11.19.0`.

Для мгновенного применения правок интерфейса запустите `Event Mosaic Local`, затем дополнительно `Event Mosaic Frontend` в IDEA и откройте <http://127.0.0.1:5173>. Эта необязательная конфигурация Vite обращается к Spring на `18080`. Для обычного запуска приложения достаточно одной `Event Mosaic Local`, карта доступна на `18080`.

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
результат. `bootRun` также подготавливает собранный frontend, но отдельный Vite удобен для мгновенного применения правок интерфейса.

## Геометрия карты

Опубликованную country-геометрию можно полностью проверить без исходных
Natural Earth данных и доступа к сети:

```powershell
# Windows: npm.cmd сохраняет передачу аргументов после -- в PowerShell.
npm.cmd --prefix tools/map-geometry run geometry:verify -- --published
```

```shell
# macOS
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
