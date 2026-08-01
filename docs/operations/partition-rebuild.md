# Аварийное восстановление и очистка поисковых данных

Эта инструкция предназначена для разработчика или администратора сервера.
Обычному пользователю приложения выполнять эти действия не нужно.

Если часть поисковых данных потеряна, повреждена или содержит лишние записи,
механизм rebuild создает новую исправленную копию, проверяет ее и только затем
переключает на нее поиск. Отдельная cleanup-команда может явно удалить одну
больше не используемую technical generation по exact именам и UUID. Ни rebuild,
ни возраст generation не запускают удаление автоматически.

После отдельного согласования production security и deployment эти операции
должны вызываться через защищенный асинхронный endpoint и понятный экран
Operator. Этот интерфейс станет основным штатным способом обслуживания, а
текущие server-side one-shot команды останутся аварийным break-glass fallback
на случай, когда Operator UI или HTTP-слой недоступны.

## Когда использовать rebuild

Запускайте восстановление только если диагностика приложения указывает на одну
из ситуаций:

- в Elasticsearch отсутствует ожидаемый фрагмент данных;
- текущая копия фрагмента повреждена;
- проверка обнаружила лишние документы.

Идентификатор фрагмента имеет вид `pYYYYMMDD`, например `p20260727`. Не
выбирайте его наугад: используйте значение из диагностики конкретного
инцидента.

## Где запускать

Команды запускаются не на компьютере обычного пользователя, а в том же
серверном окружении, где работает Event Mosaic:

- на Linux-сервере - отдельным запуском того же JAR с теми же переменными
  окружения и secrets;
- в Docker или Kubernetes - отдельным одноразовым container/job из того же
  image, с теми же network, configuration и storage mounts;
- при разработке - из корня проекта через Gradle.

Обычный web-процесс заменять ими не нужно. Каждая команда запускает отдельный
Spring process без HTTP-сервера, выполняет одну операцию и завершается.

Точная команда для Docker, Kubernetes или другого production deployment
зависит от еще не выбранной схемы развертывания. При ее появлении этот файл
нужно дополнить готовой командой для конкретной платформы. Нельзя запускать
произвольную локальную сборку против production PostgreSQL и Elasticsearch.

## Как проходит rebuild

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
- Передайте ровно один явный maintenance mode в command line. Rebuild и cleanup
  нельзя запускать одним process или включать только через environment/config.
- Создайте закрытый рабочий каталог с постоянным storage. Plan-файл в нем не
  должен существовать до inspect.
- Подготовьте идентификатор оператора и короткий код причины. Идентификатор
  допускает латинские буквы, цифры, `.`, `_`, `-`; код причины пишется
  заглавными латинскими буквами, например `SURPLUS_REPAIR`.

Новый plan по умолчанию нужно подтвердить в течение 15 минут. Он содержит
служебные сведения и локальные пути, поэтому его нельзя публиковать.

## Rebuild: шаг 1. Проверить и создать plan

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

## Rebuild: шаг 2. Выполнить восстановление

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

## Когда использовать cleanup

Cleanup предназначен только для явно выбранной technical generation со
статусом `FAILED` или `SUPERSEDED`, а также для безопасно распознанного orphan
`BUILDING`. Он не удаляет текущую `ACTIVE` generation и не является retention
по возрасту.

- Даже пустой собственный failed build не удаляется автоматически.
- Orphan `BUILDING` сначала повторно проверяется под partition lock. Только при
  истекших lease и heartbeat, неизменном owner/task/write outcome и отсутствии
  обоих exact indices во всех aliases он переводится в
  `FAILED(ORPHANED_BUILD)`. Любая alias membership или неоднозначный alias
  outcome требует lifecycle reconciliation, а не delete.
- `SUPERSEDED` разрешается очищать только при неизменной healthy current
  `ACTIVE`, валидных receipts, отсутствии открытого repair/rollback и доступных
  replay sources.

Получите `partition-key` и durable `generation-uuid` из диагностики конкретного
инцидента. Не подбирайте generation по номеру, имени index или возрасту.

## Как проходит cleanup

Очистка также намеренно разделена на два шага:

1. `INSPECT_CLEANUP` выполняет read-only проверку exact generation и создает
   новый plan-файл.
2. `CLEANUP_GENERATION` читает тот же plan, повторно проверяет fingerprint и
   только затем может начать destructive operation.

Plan фиксирует partition и generation state versions, exact physical names и
UUID, membership обоих indices во всех aliases, owner/lease и write outcome,
а для `SUPERSEDED` также защищаемую current generation, receipts и replay
evidence. Поле `observedStoreBytes` показывает суммарный размер выбранной пары
на момент inspect, но остается информационным: background merge может изменить
его без смены identity, поэтому размер не входит в authorization fingerprint.
Fingerprint связывает execute с остальным immutable evidence. Plan нельзя
редактировать или заменять между inspect и execute; по умолчанию его нужно
подтвердить в течение 15 минут.

Durable cleanup проходит только по цепочке:

```text
FAILED/SUPERSEDED -> CLEANUP_PENDING -> DELETE_REQUESTED -> CLEANED
```

Для orphan перед ней выполняется отдельный условный переход
`BUILDING -> FAILED(ORPHANED_BUILD)`. `CLEANUP_PENDING` атомарно сохраняет
operation token, expected state version и fingerprint. Непосредственно перед
`DELETE_REQUESTED` повторно проверяются token, versions, exact names/UUID,
aliases, active task и lease. До durable `DELETE_REQUESTED` устаревший worker не
может отправить delete. После этого immutable token не заменяется и не
отменяется до reconciliation.

## Cleanup: шаг 1. Проверить и создать plan

Создайте отдельный plan-файл. Он не должен существовать до inspect:

```shell
java -jar /opt/event-mosaic/event-mosaic.jar \
  --event-mosaic.maintenance.generation-cleanup.mode=INSPECT_CLEANUP \
  --event-mosaic.maintenance.generation-cleanup.partition-key=p20260727 \
  --event-mosaic.maintenance.generation-cleanup.generation-uuid=11111111-2222-3333-4444-555555555555 \
  --event-mosaic.maintenance.generation-cleanup.plan-file=/var/lib/event-mosaic/maintenance/p20260727-cleanup.json
```

После успешного inspect проверьте в JSON как минимум `partitionKey`,
`generationUuid`, `generationStatus`, exact index names/UUID, alias sets,
`writeOutcome`, `observedStoreBytes`, `expiresAt` и `fingerprint`. Для
`SUPERSEDED` дополнительно проверьте `protectedActive`, `currentReceipts` и
`replaySources`. Не продолжайте, если выбранная generation, причина инцидента
или evidence не совпадают с ожиданиями.

## Cleanup: шаг 2. Выполнить очистку

Запустите тот же JAR с созданным plan-файлом:

```shell
java -jar /opt/event-mosaic/event-mosaic.jar \
  --event-mosaic.maintenance.generation-cleanup.mode=CLEANUP_GENERATION \
  --event-mosaic.maintenance.generation-cleanup.plan-file=/var/lib/event-mosaic/maintenance/p20260727-cleanup.json \
  --event-mosaic.maintenance.generation-cleanup.actor=operator-name \
  --event-mosaic.maintenance.generation-cleanup.reason-code=FAILED_BUILD_CLEANUP
```

Удаляются только два exact planned target без wildcard. Metadata становится
`CLEANED` только после подтвержденного отсутствия обоих indices. Exit code `0`
означает завершенный inspect или cleanup. Stale plan, conflict, deferred
maintenance, ownership loss и другой незавершенный outcome дают ненулевой exit
code.

## Если cleanup прервался

- Не редактируйте plan и не удаляйте indices или aliases вручную.
- После освобождения maintenance lease повторите `CLEANUP_GENERATION` с тем же
  plan, actor и reason code. Начатая operation сохраняет прежний immutable
  token.
- В `DELETE_REQUESTED` каждый exact target проверяется отдельно. Отсутствие
  expected UUID означает завершенный delete этого target; тот же UUID разрешает
  безопасный retry только после повторной проверки aliases; другой UUID дает
  conflict без delete.
- Если удален только один index пары, operation остается resumable
  `DELETE_REQUESTED`. `CLEANED` появится только после отсутствия обоих planned
  indices.
- При `CLEANUP_MAINTENANCE_BUSY` или `CLEANUP_OWNERSHIP_LOST` не запускайте
  конкурирующую operation. Дождитесь освобождения lease и повторите тот же
  execute.
- При `STALE_CLEANUP_PLAN`, `CLEANUP_EXACT_TARGET_CONFLICT`,
  `CLEANUP_ALIAS_CONFLICT` или `CLEANUP_RECONCILIATION_REQUIRED` остановитесь,
  проверьте lifecycle state и устраните расхождение. Новый inspect выполняйте
  только после reconciliation.

Background cleanup scheduler отсутствует. Обычный startup, возраст generation,
disk pressure и завершение rebuild не удаляют историю.

## Rebuild при локальной разработке

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

## Cleanup при локальной разработке

На Windows inspect можно запустить так:

```powershell
New-Item -ItemType Directory -Force -Path C:\tmp\event-mosaic-cleanup

.\gradlew.bat bootRun --args="--event-mosaic.maintenance.generation-cleanup.mode=INSPECT_CLEANUP --event-mosaic.maintenance.generation-cleanup.partition-key=p20260727 --event-mosaic.maintenance.generation-cleanup.generation-uuid=11111111-2222-3333-4444-555555555555 --event-mosaic.maintenance.generation-cleanup.plan-file=C:\tmp\event-mosaic-cleanup\p20260727.json"
```

На macOS или Linux из исходников:

```shell
mkdir -p /tmp/event-mosaic-cleanup

sh ./gradlew bootRun --args='--event-mosaic.maintenance.generation-cleanup.mode=INSPECT_CLEANUP --event-mosaic.maintenance.generation-cleanup.partition-key=p20260727 --event-mosaic.maintenance.generation-cleanup.generation-uuid=11111111-2222-3333-4444-555555555555 --event-mosaic.maintenance.generation-cleanup.plan-file=/tmp/event-mosaic-cleanup/p20260727.json'
```

Для execute замените mode на `CLEANUP_GENERATION`, уберите `partition-key` и
`generation-uuid`, затем добавьте `actor` и `reason-code`, как в серверном
примере cleanup выше.
