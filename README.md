# Event Mosaic

Event Mosaic - приложение для загрузки, обработки и отображения событий GDELT на карте.

## Стек

- Java 25
- Spring Boot 4.1.0
- Gradle 9.5.1
- PostgreSQL
- Flyway
- Elasticsearch

## Инфраструктура

Локально:

```shell
docker compose -f compose.local.yml up -d
```

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
