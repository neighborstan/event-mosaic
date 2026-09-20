[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Этот скрипт поднимает только PostgreSQL и Elasticsearch для локального review.
# Backend и frontend затем запускаются одной конфигурацией "Event Mosaic Local" в IDEA.
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ComposeFile = Join-Path $ProjectRoot "compose.yml"

# Отдельное имя Compose-проекта изолирует контейнеры, сеть и Docker volumes.
# Обычный локальный стек проекта с базой event_mosaic вообще не используется.
$ComposeProject = "event-mosaic-map04b-review"
$ElasticsearchOrigin = "http://127.0.0.1:19200"
$LegacyReviewEventIndex = "gdelt-events-v1-p19700101-g9999"
$EventReadAlias = "gdelt-events-read"

# Эти значения нужны compose.yml. Они действуют только во время скрипта,
# а в finally восстанавливаются, поэтому глобальный .env создавать не требуется.
$ReviewEnvironment = [ordered]@{
    EVENT_MOSAIC_POSTGRES_DB              = "event_mosaic_map04b_review"
    EVENT_MOSAIC_POSTGRES_USER            = "event_mosaic"
    EVENT_MOSAIC_POSTGRES_PASSWORD        = "event_mosaic"
    EVENT_MOSAIC_POSTGRES_PORT            = "15432"
    EVENT_MOSAIC_ELASTICSEARCH_HTTP_PORT  = "19200"
    EVENT_MOSAIC_ES_JAVA_OPTS             = "-Xms512m -Xmx512m"
}

function Assert-CommandAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Команда '$Name' не найдена. Установите и запустите Docker Desktop."
    }
}

function Assert-ApplicationPortAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port,

        [Parameter(Mandatory = $true)]
        [string]$ApplicationName
    )

    # Пробуем занять порт на мгновение. Уже запущенные процессы скрипт не завершает.
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        $Port
    )
    try {
        $listener.Start()
    }
    catch {
        throw "Порт $Port уже занят. Остановите прежний $ApplicationName и повторите запуск."
    }
    finally {
        $listener.Stop()
    }
}

function Invoke-ReviewCompose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ComposeArguments
    )

    & docker compose `
        --project-name $ComposeProject `
        -f $ComposeFile `
        @ComposeArguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose завершился с кодом $LASTEXITCODE."
    }
}

function Invoke-Elasticsearch {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("GET", "PUT", "POST")]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [string]$BodyJson,

        [switch]$AllowNotFound
    )

    $request = @{
        Uri         = "$ElasticsearchOrigin$Path"
        Method      = $Method
        TimeoutSec  = 5
        ErrorAction = "Stop"
    }
    if ($PSBoundParameters.ContainsKey("BodyJson")) {
        $request.ContentType = "application/json"
        $request.Body = $BodyJson
    }

    try {
        return Invoke-RestMethod @request
    }
    catch {
        $statusCode = $null
        if ($null -ne $_.Exception.Response) {
            try {
                $statusCode = [int]$_.Exception.Response.StatusCode
            }
            catch {
                $statusCode = $null
            }
        }
        if ($AllowNotFound -and $statusCode -eq 404) {
            return $null
        }
        throw
    }
}

function Wait-ElasticsearchReady {
    # Контейнер уже запущен, но Elasticsearch еще несколько секунд поднимает HTTP API.
    # Ждем не красное состояние, максимум одну минуту.
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            $health = Invoke-Elasticsearch -Method GET -Path "/_cluster/health"
            if ($health.status -ne "red") {
                return
            }
        }
        catch {
            # Короткие ошибки соединения во время старта ожидаемы.
        }
        Start-Sleep -Seconds 1
    }

    throw "Elasticsearch не стал готов за 60 секунд. Проверьте Docker Desktop."
}

function Disconnect-EmptyLegacyReviewIndex {
    # Старый сценарий подключал пустую заглушку к поиску до запуска Spring.
    # Ее дата не соответствует недельным разделам и блокирует автозагрузку.
    $legacyAlias = Invoke-Elasticsearch `
        -Method GET `
        -Path "/$LegacyReviewEventIndex/_alias/$EventReadAlias" `
        -AllowNotFound
    if ($null -eq $legacyAlias) {
        return
    }

    $documents = Invoke-Elasticsearch -Method GET -Path "/$LegacyReviewEventIndex/_count"
    if ($documents._shards.failed -ne 0 -or $documents.count -ne 0) {
        throw "Старая заглушка $LegacyReviewEventIndex содержит документы или не проверена полностью. Остановлено без изменения данных; требуется отдельная проверка этого индекса."
    }

    # Снимаем только одну известную привязку. Сам индекс и остальные aliases сохраняются.
    $aliasJson = @{
        actions = @(
            @{
                remove = @{
                    index = $LegacyReviewEventIndex
                    alias = $EventReadAlias
                    must_exist = $true
                }
            }
        )
    } | ConvertTo-Json -Depth 6 -Compress
    Invoke-Elasticsearch -Method POST -Path "/_aliases" -BodyJson $aliasJson | Out-Null
    Write-Host "Пустая заглушка прежнего сценария отключена от поиска; данные сохранены."
}

function Set-ReviewEnvironment {
    $original = [ordered]@{}
    foreach ($entry in $ReviewEnvironment.GetEnumerator()) {
        $original[$entry.Key] = [Environment]::GetEnvironmentVariable(
            $entry.Key,
            "Process"
        )
        [Environment]::SetEnvironmentVariable(
            $entry.Key,
            [string]$entry.Value,
            "Process"
        )
    }
    return $original
}

function Restore-Environment {
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$OriginalEnvironment
    )

    foreach ($entry in $OriginalEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable(
            [string]$entry.Key,
            $entry.Value,
            "Process"
        )
    }
}

Assert-CommandAvailable -Name "docker"
Assert-ApplicationPortAvailable -Port 18080 -ApplicationName "backend"

# Node.js/npm проверяет Gradle при запуске приложения; подготовке контейнеров они не нужны.

$originalEnvironment = Set-ReviewEnvironment
Push-Location $ProjectRoot
try {
    # Проверяем Docker daemon до создания каких-либо review-ресурсов.
    & docker info --format "{{.ServerVersion}}" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop не отвечает. Запустите его и повторите команду."
    }

    # На первом запуске инфраструктурные порты должны быть свободны. При
    # повторном start их уже занимают наши review-контейнеры, и это нормально.
    $runningReviewServices = @(
        & docker compose `
            --project-name $ComposeProject `
            -f $ComposeFile `
            ps `
            --services `
            --status running
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Не удалось проверить состояние review-контейнеров."
    }
    if ("postgres" -notin $runningReviewServices) {
        Assert-ApplicationPortAvailable `
            -Port 15432 `
            -ApplicationName "review PostgreSQL"
    }
    if ("elasticsearch" -notin $runningReviewServices) {
        Assert-ApplicationPortAvailable `
            -Port 19200 `
            -ApplicationName "review Elasticsearch"
    }

    Write-Host "Запускаю изолированные PostgreSQL и Elasticsearch..."
    Invoke-ReviewCompose -ComposeArguments @("up", "-d", "--wait")
    Wait-ElasticsearchReady

    Disconnect-EmptyLegacyReviewIndex
    # Spring сам устанавливает templates, создает рабочие индексы и подключает
    # их к поиску. До первого успешного цикла временная недоступность данных ожидаема.

    Write-Host ""
    Write-Host "Инфраструктура готова."
    Write-Host "1. В IDEA выберите конфигурацию 'Event Mosaic Local' и нажмите Run."
    Write-Host "2. Дождитесь строки 'Started EventMosaicApplication'."
    Write-Host "3. Откройте http://127.0.0.1:18080 (карта и API без Vite)."
    Write-Host "   На пустом хранилище дождитесь загрузки GDELT и обновите страницу."
    Write-Host "4. Для обычной остановки нажмите Stop в IDEA; данные и контейнеры сохраняются."
    Write-Host "   stop-map-review.ps1 нужен только для явного удаления проверочного стека и его данных."
}
finally {
    Pop-Location
    Restore-Environment -OriginalEnvironment $originalEnvironment
}
