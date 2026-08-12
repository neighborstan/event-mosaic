[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Этот скрипт поднимает только PostgreSQL и Elasticsearch для локального review.
# Backend и frontend затем запускаются одной конфигурацией "Event Mosaic Local" в IDEA.
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ComposeFile = Join-Path $ProjectRoot "compose.yml"
$EventTemplateFile = Join-Path `
    $ProjectRoot `
    "src\main\resources\elasticsearch\gdelt-events-v1-template.json"

# Отдельное имя Compose-проекта изолирует контейнеры, сеть и Docker volumes.
# Обычный локальный стек проекта с базой event_mosaic вообще не используется.
$ComposeProject = "event-mosaic-map04b-review"
$ElasticsearchOrigin = "http://127.0.0.1:19200"
$EventTemplateName = "gdelt-events-v1-template"
$ReviewEventIndex = "gdelt-events-v1-p19700101-g9999"
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
Assert-ApplicationPortAvailable -Port 5173 -ApplicationName "frontend"

# Shared IDEA-конфигурация использует проверенный project-local Node.js.
# Этот check дает понятную ошибку до попытки запуска npm внутри IDEA.
$NodeExecutable = Join-Path $ProjectRoot ".local\node-v24.19.0-win-x64\node.exe"
if (-not (Test-Path -LiteralPath $NodeExecutable -PathType Leaf)) {
    throw "Не найден $NodeExecutable. Сначала выполните обычный bootstrap проекта."
}

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

    # Пустой Elasticsearch сам по себе недостаточен: snapshot читает стабильный
    # alias и ожидает production mapping. Поэтому ставим tracked Event template.
    Write-Host "Устанавливаю Event index template..."
    $templateJson = Get-Content -LiteralPath $EventTemplateFile -Raw
    Invoke-Elasticsearch `
        -Method PUT `
        -Path "/_index_template/$EventTemplateName" `
        -BodyJson $templateJson | Out-Null

    # Индекс пустой, поэтому на карте будут честные нулевые счетчики и UNKNOWN
    # coverage. Имя подходит под template и существует только в отдельном volume.
    $existingIndex = Invoke-Elasticsearch `
        -Method GET `
        -Path "/$ReviewEventIndex" `
        -AllowNotFound
    if ($null -eq $existingIndex) {
        Write-Host "Создаю пустой Event index..."
        $createIndexJson = @{
            aliases = @{
                $EventReadAlias = @{}
            }
        } | ConvertTo-Json -Depth 5 -Compress
        Invoke-Elasticsearch `
            -Method PUT `
            -Path "/$ReviewEventIndex" `
            -BodyJson $createIndexJson | Out-Null
    }
    else {
        # Повторный start идемпотентен: возвращаем alias, если его сняли вручную.
        $aliasJson = @{
            actions = @(
                @{
                    add = @{
                        index = $ReviewEventIndex
                        alias = $EventReadAlias
                    }
                }
            )
        } | ConvertTo-Json -Depth 6 -Compress
        Invoke-Elasticsearch `
            -Method POST `
            -Path "/_aliases" `
            -BodyJson $aliasJson | Out-Null
    }

    Write-Host ""
    Write-Host "Инфраструктура готова."
    Write-Host "1. В IDEA выберите конфигурацию 'Event Mosaic Local' и нажмите Run."
    Write-Host "2. Дождитесь строки 'Started EventMosaicApplication'."
    Write-Host "3. Откройте http://127.0.0.1:5173"
    Write-Host "4. После проверки остановите Run в IDEA и запустите stop-map-review.ps1"
}
finally {
    Pop-Location
    Restore-Environment -OriginalEnvironment $originalEnvironment
}
