# Локальная проверка карты

Для запуска не нужен `.env`: все несекретные локальные параметры уже записаны
в shared-конфигурациях IDEA из каталога `.run`.

1. Запустить из корня проекта:

   ```powershell
   .\tools\map-review\start-map-review.ps1
   ```

2. В IDEA выбрать конфигурацию `Event Mosaic Local` и нажать `Run`. Она одновременно
   запускает Spring backend на порту `18080` и Vite frontend на порту `5173`.

3. Открыть <http://127.0.0.1:5173>.

4. После проверки сначала остановить `Event Mosaic Local` в IDEA, затем выполнить:

   ```powershell
   .\tools\map-review\stop-map-review.ps1
   ```

Start-скрипт поднимает отдельный Docker Compose-проект
`event-mosaic-map04b-review`: PostgreSQL доступен на порту `15432`, а
Elasticsearch - на `19200`. Его контейнеры, сеть и volumes не пересекаются с
обычным локальным стеком. Stop-скрипт удаляет целиком только этот review-стек и
его временные данные.
