# Подготовка геометрии карты

Утилита готовит, проверяет и публикует неизменяемую country-геометрию для
Event Mosaic. Она живет отдельно от Spring Boot: обычные Gradle-сборка, запуск
приложения и HTTP-запросы не устанавливают Node.js-зависимости, не скачивают
Natural Earth и не регенерируют GeoJSON.

## Требования

- Node.js `24.19.0`;
- npm `11.17.0`;
- запуск команд из корня репозитория.

Установить зафиксированные зависимости без npm lifecycle scripts:

```shell
npm --prefix tools/map-geometry ci --ignore-scripts --no-audit --no-fund
```

## Поддерживаемые команды

Команды одинаковы в PowerShell на Windows и в zsh на macOS:

```shell
# Построить candidate выбранного профиля. Команда может скачать pinned inputs.
npm --prefix tools/map-geometry run geometry:prepare

# Построить candidate другого профиля.
npm --prefix tools/map-geometry run geometry:prepare -- --profile retain-20-p4

# Проверить default candidate без сети и raw inputs.
npm --prefix tools/map-geometry run geometry:verify

# Проверить явно выбранный каталог candidate.
npm --prefix tools/map-geometry run geometry:verify -- --directory <candidate-path>

# Опубликовать candidate из selectedProfileId без замены существующей версии.
npm --prefix tools/map-geometry run geometry:publish

# Проверить уже опубликованную пару без сети и запуска mapshaper.
npm --prefix tools/map-geometry run geometry:verify -- --published
```

`geometry:prepare` является единственной командой, которая получает исходные
данные. Она всегда проверяет их размер и SHA-256 до разбора. `geometry:verify`
читает только пару `countries.geojson` и `manifest.json`, а
`geometry:publish` использует только профиль `selectedProfileId` из
`data/geometry-config.json`.

Тесты также работают на локальных компактных fixtures без runtime internet:

```shell
npm --prefix tools/map-geometry test
```

## Обычная сборка без подготовки геометрии

Подготовка и публикация не подключены к Gradle. Поддерживаемые обычные команды:

```powershell
# Windows
.\gradlew.bat test
.\gradlew.bat build
```

```shell
# macOS
sh ./gradlew test
sh ./gradlew build
```

Если Gradle dependencies и нужные Testcontainers images уже находятся в
локальных кешах, Gradle dependency resolution можно дополнительно запретить:

```powershell
# Windows
.\gradlew.bat --offline test
.\gradlew.bat --offline build
```

```shell
# macOS
sh ./gradlew --offline test
sh ./gradlew --offline build
```

Флаг `--offline` запрещает загрузки Gradle, но не управляет Docker. Для
полностью отключенной сети необходимые PostgreSQL, Elasticsearch и служебные
Testcontainers images тоже должны быть доступны локально заранее.

## Выпуск новой версии

Существующий каталог `src/main/resources/static/map/geometry/country-vN/`
неизменяем. Новые bytes нельзя публиковать под прежней `geometryVersion`.

1. Выбрать следующий неиспользованный `country-vN` в
   `data/geometry-config.json`.
2. При реальном изменении источников, crosswalk, tools или параметров обновить
   соответствующие versioned данные и точный provenance. Старый опубликованный
   каталог оставить без изменений.
3. Выполнить `geometry:prepare` для выбранного профиля.
4. Выполнить `geometry:verify` для candidate и проверить метрики.
5. Выполнить `geometry:publish`.
6. Выполнить `geometry:verify -- --published` и Spring/Gradle-тесты.

Номер версии увеличивается при изменении идентичности регионов, набора
полигонов, crosswalk semantics, параметров упрощения или других bytes
публичной пары. Обновление исходного URL или версии инструмента без изменения
результата все равно должно остаться явно отражено в manifest новой версии,
если этот provenance входит в публикуемый contract.

## Безопасное восстановление

- Ошибка `prepare` или `verify` не меняет опубликованную пару. Исправить
  причину и повторить команду; не копировать candidate в static resources
  вручную.
- После прерванного `publish` сначала выполнить
  `geometry:verify -- --published`. Успех означает, что exact pair уже
  опубликована. Если versioned target отсутствует, тот же `publish` можно
  повторить идемпотентно.
- Если target существует частично или содержит другие bytes, остановиться. Не
  дополнять, не удалять и не перезаписывать его автоматически. Восстановить
  доверенную tracked-пару после review либо выбрать новую `geometryVersion`.
- После аварийного завершения может остаться точный lock-файл
  `src/main/resources/static/map/geometry/.country-vN.publish.lock`. Удалять
  только этот конкретный lock можно лишь после проверки, что другой publisher
  не работает и состояние target уже проверено. Wildcard и очистку всего
  каталога geometry использовать нельзя.
- Собственные временные каталоги publisher очищает сам. Чужой или замененный
  путь он намеренно оставляет без удаления, чтобы не уничтожить неизвестные
  данные.
