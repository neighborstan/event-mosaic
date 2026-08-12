[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Сначала остановите в IDEA конфигурацию "Event Mosaic Local".
# Затем этот скрипт удалит только отдельный review Compose-проект и его volumes.
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$ComposeFile = Join-Path $ProjectRoot "compose.yml"
$ComposeProject = "event-mosaic-map04b-review"

# compose.yml требует эти значения даже при down. Они задаются только в процессе
# скрипта и восстанавливаются в finally; пользовательский .env не нужен.
$ReviewEnvironment = [ordered]@{
    EVENT_MOSAIC_POSTGRES_DB              = "event_mosaic_map04b_review"
    EVENT_MOSAIC_POSTGRES_USER            = "event_mosaic"
    EVENT_MOSAIC_POSTGRES_PASSWORD        = "event_mosaic"
    EVENT_MOSAIC_POSTGRES_PORT            = "15432"
    EVENT_MOSAIC_ELASTICSEARCH_HTTP_PORT  = "19200"
    EVENT_MOSAIC_ES_JAVA_OPTS             = "-Xms512m -Xmx512m"
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

if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Команда 'docker' не найдена. Установите и запустите Docker Desktop."
}

$originalEnvironment = Set-ReviewEnvironment
Push-Location $ProjectRoot
try {
    & docker info --format "{{.ServerVersion}}" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop не отвечает. Запустите его и повторите команду."
    }

    Write-Host "Удаляю изолированные review-контейнеры и их временные volumes..."
    & docker compose `
        --project-name $ComposeProject `
        -f $ComposeFile `
        down `
        --volumes `
        --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose down завершился с кодом $LASTEXITCODE."
    }

    Write-Host ""
    Write-Host "Очистка завершена. Обычные контейнеры и данные проекта не менялись."
}
finally {
    Pop-Location
    Restore-Environment -OriginalEnvironment $originalEnvironment
}
