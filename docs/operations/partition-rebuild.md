# Аварийное восстановление поисковых данных

Эта инструкция предназначена для разработчика или администратора сервера.
Обычному пользователю приложения выполнять эти действия не нужно.

Если часть поисковых данных потеряна, повреждена или содержит лишние записи,
механизм rebuild создает новую исправленную копию, проверяет ее и только затем
переключает на нее поиск. Старая исправная копия не удаляется автоматически.

В будущей админке этот механизм должен вызываться через защищенный endpoint и
понятный экран управления. Текущая серверная команда остается аварийным
вариантом на случай, когда админка или HTTP-слой недоступны.

## Когда использовать

Запускайте восстановление только если диагностика приложения указывает на одну
из ситуаций:

- в Elasticsearch отсутствует ожидаемый фрагмент данных;
- текущая копия фрагмента повреждена;
- проверка обнаружила лишние документы.

Идентификатор фрагмента имеет вид `pYYYYMMDD`, например `p20260727`. Не
выбирайте его наугад: используйте значение из диагностики конкретного
инцидента.

## Где запускать

Команда запускается не на компьютере обычного пользователя, а в том же
серверном окружении, где работает Event Mosaic:

- на Linux-сервере - отдельным запуском того же JAR с теми же переменными
  окружения и secrets;
- в Docker или Kubernetes - отдельным одноразовым container/job из того же
  image, с теми же network, configuration и storage mounts;
- при разработке - из корня проекта через Gradle.

Обычный web-процесс заменять этой командой не нужно. Команда запускает отдельный
Spring process без HTTP-сервера, выполняет одну операцию и завершается.

Точная команда для Docker, Kubernetes или другого production deployment
зависит от еще не выбранной схемы развертывания. При ее появлении этот файл
нужно дополнить готовой командой для конкретной платформы. Нельзя запускать
произвольную локальную сборку против production PostgreSQL и Elasticsearch.

## Как проходит восстановление

Восстановление намеренно разделено на два шага:

1. `INSPECT_REBUILD` безопасно проверяет ситуацию и создает plan-файл. Данные в
   PostgreSQL и Elasticsearch не изменяются.
2. `REBUILD_PARTITION` принимает тот же plan-файл, повторно проверяет состояние
   и выполняет восстановление.

Plan-файл защищает от восстановления не того фрагмента или от выполнения уже
устаревшего решения. Не редактируйте его вручную и не используйте другой файл
между двумя шагами.

## Перед запуском

- Используйте тот же runtime configuration, что и серверное приложение.
- Убедитесь, что PostgreSQL, Elasticsearch и staging доступны.
- Оставьте `event-mosaic.ingestion.gdelt.one-shot-enabled=false`.
- Создайте закрытый рабочий каталог с постоянным storage. Plan-файл в нем не
  должен существовать до inspect.
- Подготовьте идентификатор оператора и короткий код причины. Идентификатор
  допускает латинские буквы, цифры, `.`, `_`, `-`; код причины пишется
  заглавными латинскими буквами, например `SURPLUS_REPAIR`.

Новый plan по умолчанию нужно подтвердить в течение 15 минут. Он содержит
служебные сведения и локальные пути, поэтому его нельзя публиковать.

## Шаг 1. Проверить и создать plan

Пример для развернутого JAR на Linux. Пути и partition key замените значениями
своего окружения:

```shell
mkdir -p /var/lib/event-mosaic/maintenance

java -jar /opt/event-mosaic/event-mosaic.jar \
  --event-mosaic.maintenance.partition-rebuild.mode=INSPECT_REBUILD \
  --event-mosaic.maintenance.partition-rebuild.partition-key=p20260727 \
  --event-mosaic.maintenance.partition-rebuild.plan-file=/var/lib/event-mosaic/maintenance/p20260727.json
```

После успешного inspect откройте JSON-файл и убедитесь, что `partitionKey` и
`repairCause` соответствуют инциденту. Если команда сообщает, что rebuild не
нужен, исходные архивы недоступны или уже идет другая операция, продолжать
нельзя.

## Шаг 2. Выполнить восстановление

Запустите тот же JAR с созданным plan-файлом:

```shell
java -jar /opt/event-mosaic/event-mosaic.jar \
  --event-mosaic.maintenance.partition-rebuild.mode=REBUILD_PARTITION \
  --event-mosaic.maintenance.partition-rebuild.plan-file=/var/lib/event-mosaic/maintenance/p20260727.json \
  --event-mosaic.maintenance.partition-rebuild.actor=operator-name \
  --event-mosaic.maintenance.partition-rebuild.reason-code=SURPLUS_REPAIR
```

Exit code `0` означает, что проверка или восстановление завершились. Ненулевой
exit code означает, что операция не завершена и результат нельзя считать
успешным.

## Если команда прервалась

- Не редактируйте plan-файл и не переключайте Elasticsearch aliases вручную.
- Если execute уже начал операцию, после освобождения maintenance lease
  повторите `REBUILD_PARTITION` с тем же plan, actor и reason code.
- Если операция еще принадлежит другому процессу, команда завершится с
  `MAINTENANCE_DEFERRED`. Подождите и повторите тот же execute.
- Если execute не начинался, а plan устарел, создайте новый plan через inspect.
- При неожиданном target/UUID или поврежденных исходных архивах остановитесь и
  сначала устраните причину. Не используйте wildcard delete.

## Запуск при локальной разработке

Из корня проекта на Windows inspect можно запустить так:

```powershell
New-Item -ItemType Directory -Force -Path C:\tmp\event-mosaic-rebuild

.\gradlew.bat bootRun --args="--event-mosaic.maintenance.partition-rebuild.mode=INSPECT_REBUILD --event-mosaic.maintenance.partition-rebuild.partition-key=p20260727 --event-mosaic.maintenance.partition-rebuild.plan-file=C:\tmp\event-mosaic-rebuild\p20260727.json"
```

На macOS или Linux из исходников используется тот же набор application
arguments:

```shell
mkdir -p /tmp/event-mosaic-rebuild

sh ./gradlew bootRun --args='--event-mosaic.maintenance.partition-rebuild.mode=INSPECT_REBUILD --event-mosaic.maintenance.partition-rebuild.partition-key=p20260727 --event-mosaic.maintenance.partition-rebuild.plan-file=/tmp/event-mosaic-rebuild/p20260727.json'
```

Для execute замените mode на `REBUILD_PARTITION`, уберите `partition-key` и
добавьте `actor` и `reason-code`, как в серверном примере выше.
