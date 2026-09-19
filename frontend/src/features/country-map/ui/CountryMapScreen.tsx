import { useEffect, useMemo, useRef, useState } from "react";
import type { JSX } from "react";

import {
  useCountryGeometry,
  type CountryGeometryLoader,
} from "../api/useCountryGeometry";
import {
  useCountrySnapshot,
  type CountrySnapshotClock,
  type CountrySnapshotLoader,
  type CountrySnapshotRequestState,
} from "../api/useCountrySnapshot";
import {
  resolveBasemapProvider,
  type BasemapConfiguration,
} from "../lib/basemapProvider";
import {
  useMapLibreMap,
  type MapLibreMapFactory,
  type MapLibreTechnicalState,
} from "../lib/useMapLibreMap";
import {
  buildCountryVisualModel,
  COUNTRY_TONE_SUMMARIES,
  COUNTRY_VOLUME_LEVELS,
  COUNTRY_EMPTY_COLOR,
  COUNTRY_MISSING_TONE_COLOR,
  type CountryMapDisplayMode,
  type CountryToneVisualGroup,
  type CountryVisualRegion,
} from "../model/countryVisualModel";

type CountryMapScreenProps = Readonly<{
  providerValue?: string;
  geometryLoader?: CountryGeometryLoader | undefined;
  snapshotLoader?: CountrySnapshotLoader | undefined;
  snapshotClock?: CountrySnapshotClock | undefined;
  snapshotRefreshIntervalMilliseconds?: number | undefined;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

type CountryMapCanvasProps = Readonly<{
  configuration: BasemapConfiguration;
  geometryLoader?: CountryGeometryLoader | undefined;
  snapshotLoader?: CountrySnapshotLoader | undefined;
  snapshotClock?: CountrySnapshotClock | undefined;
  snapshotRefreshIntervalMilliseconds?: number | undefined;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

type CountryMapTechnicalState =
  MapLibreTechnicalState | Readonly<{ status: "geometry-error" }>;

export function CountryMapScreen({
  providerValue = import.meta.env.VITE_MAP_BASEMAP_PROVIDER,
  geometryLoader,
  snapshotLoader,
  snapshotClock,
  snapshotRefreshIntervalMilliseconds,
  mapFactory,
}: CountryMapScreenProps = {}): JSX.Element {
  const providerResolution = resolveBasemapProvider(providerValue);

  return (
    <main className="app-shell">
      <article
        className="country-map-shell"
        aria-labelledby="country-map-title"
      >
        <header className="country-map-shell__header">
          <div>
            <p className="country-map-shell__eyebrow">Event Mosaic</p>
            <h1 id="country-map-title">Карта событий по странам</h1>
          </div>
          <p className="country-map-shell__intro">
            Тональность и количество событий за последние 24 часа. Обобщенная
            картина по странам.
          </p>
        </header>
        {providerResolution.status === "resolved" ? (
          <CountryMapCanvas
            configuration={providerResolution.configuration}
            geometryLoader={geometryLoader}
            snapshotLoader={snapshotLoader}
            snapshotClock={snapshotClock}
            snapshotRefreshIntervalMilliseconds={
              snapshotRefreshIntervalMilliseconds
            }
            mapFactory={mapFactory}
          />
        ) : (
          <section
            className="country-map-shell__map-region country-map-shell__map-region--error"
            aria-label="Область карты стран"
          >
            <p className="country-map-shell__alert" role="alert">
              {providerResolution.error.message}
            </p>
          </section>
        )}
      </article>
    </main>
  );
}

function CountryMapCanvas({
  configuration,
  geometryLoader,
  snapshotLoader,
  snapshotClock,
  snapshotRefreshIntervalMilliseconds,
  mapFactory,
}: CountryMapCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const [displayMode, setDisplayMode] = useState<CountryMapDisplayMode>("tone");
  const [hoveredRegionId, setHoveredRegionId] = useState<string | null>(null);
  const [selectedRegionId, setSelectedRegionId] = useState<string | null>(null);
  const geometryState = useCountryGeometry({ loader: geometryLoader });
  const geometry =
    geometryState.status === "loaded" ? geometryState.geometry : null;
  const snapshotState = useCountrySnapshot({
    geometry,
    loader: snapshotLoader,
    clock: snapshotClock,
    refreshIntervalMilliseconds: snapshotRefreshIntervalMilliseconds,
  });
  const joinedSnapshot =
    snapshotState.status === "accepted" ? snapshotState.joinedSnapshot : null;
  const visualRegions = useMemo(
    () =>
      joinedSnapshot === null
        ? null
        : buildCountryVisualModel(joinedSnapshot, displayMode),
    [joinedSnapshot, displayMode],
  );
  const visualRegionsById = useMemo(
    () =>
      new Map(
        (visualRegions ?? []).map(
          (region) => [region.regionId, region] as const,
        ),
      ),
    [visualRegions],
  );
  const hoveredRegion =
    hoveredRegionId === null
      ? null
      : (visualRegionsById.get(hoveredRegionId) ?? null);
  const selectedRegion =
    selectedRegionId === null
      ? null
      : (visualRegionsById.get(selectedRegionId) ?? null);

  const mapState = useMapLibreMap({
    containerRef,
    style: configuration.styleUrl,
    attributionControl: configuration.attributionControl,
    geometry,
    visualRegions,
    hoveredRegionId,
    selectedRegionId,
    onHoverRegion: (regionId) => {
      setHoveredRegionId(
        regionId !== null && visualRegionsById.has(regionId) ? regionId : null,
      );
    },
    onSelectRegion: (regionId) => {
      setSelectedRegionId(
        regionId !== null && visualRegionsById.has(regionId) ? regionId : null,
      );
    },
    mapFactory,
  });
  const technicalState = resolveTechnicalState(mapState, geometryState.status);

  useEffect(() => {
    if (selectedRegionId === null) {
      return undefined;
    }

    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setSelectedRegionId(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [selectedRegionId]);

  return (
    <section
      className="country-map-shell__map-region"
      aria-label="Область карты стран"
      aria-busy={technicalState.status === "loading"}
    >
      <div className="country-map-shell__toolbar">
        <div
          className="country-map-shell__modes"
          role="group"
          aria-label="Что показать на карте"
        >
          <button
            type="button"
            aria-pressed={displayMode === "tone"}
            onClick={() => setDisplayMode("tone")}
          >
            Тональность
          </button>
          <button
            type="button"
            aria-pressed={displayMode === "volume"}
            onClick={() => setDisplayMode("volume")}
          >
            Количество событий
          </button>
        </div>
        <label className="country-map-shell__country-picker">
          Страна
          <select
            value={selectedRegionId ?? ""}
            disabled={visualRegions === null}
            onChange={(event) =>
              setSelectedRegionId(event.target.value || null)
            }
          >
            <option value="">Выберите на карте или в списке</option>
            {[...(visualRegions ?? [])]
              .sort((left, right) =>
                left.geometry.properties.displayName.localeCompare(
                  right.geometry.properties.displayName,
                ),
              )
              .map((region) => (
                <option key={region.regionId} value={region.regionId}>
                  {region.geometry.properties.displayName}
                </option>
              ))}
          </select>
        </label>
      </div>
      {snapshotState.status === "accepted" ? (
        <CountryToneLegend mode={displayMode} />
      ) : null}
      <div className="country-map-shell__map-stage">
        <div
          ref={containerRef}
          className="country-map-shell__canvas"
          role="region"
          aria-label="Интерактивная карта мира"
        />
        {hoveredRegion !== null && snapshotState.status === "accepted" ? (
          <CountryHoverSummary
            region={hoveredRegion}
            coverageStatus={
              snapshotState.joinedSnapshot.snapshot.coverage.status
            }
          />
        ) : null}
        {selectedRegion !== null && snapshotState.status === "accepted" ? (
          <CountrySelectedSummary
            region={selectedRegion}
            state={snapshotState}
            onClose={() => {
              setSelectedRegionId(null);
            }}
          />
        ) : null}
      </div>
      <MapTechnicalStatus state={technicalState} />
      <SnapshotTechnicalStatus state={snapshotState} />
      <p className="country-map-shell__scope-note">
        Цвет относится ко всей стране. При приближении места отдельных событий
        пока не показываются.
      </p>
    </section>
  );
}

function MapTechnicalStatus({
  state,
}: Readonly<{ state: CountryMapTechnicalState }>): JSX.Element {
  switch (state.status) {
    case "loading":
      return (
        <p
          className="country-map-shell__status country-map-shell__status--map"
          role="status"
          aria-label="Состояние карты"
          aria-live="polite"
        >
          Карта загружается. Ожидаем фоновый слой и проверенные границы стран.
        </p>
      );
    case "ready":
      return (
        <p
          className="country-map-shell__status country-map-shell__status--map"
          role="status"
          aria-label="Состояние карты"
          aria-live="polite"
        >
          Карта готова. Показаны границы 258 стран и территорий.
        </p>
      );
    case "provider-error":
      return (
        <p
          className="country-map-shell__alert"
          role="alert"
          aria-label="Ошибка фоновой карты"
        >
          Не удалось загрузить выбранную фоновую карту. Автоматическое
          переключение источника не выполняется.
        </p>
      );
    case "geometry-error":
      return (
        <p
          className="country-map-shell__alert"
          role="alert"
          aria-label="Ошибка геометрии стран"
        >
          Не удалось загрузить или проверить геометрию стран. Поврежденные
          данные не отображаются как пустая карта.
        </p>
      );
  }
}

function SnapshotTechnicalStatus({
  state,
}: Readonly<{ state: CountrySnapshotRequestState }>): JSX.Element {
  if (state.status === "loading") {
    return (
      <p
        className="country-map-shell__status"
        role="status"
        aria-label="Состояние данных событий"
        aria-live="polite"
      >
        Данные событий загружаются и проверяются отдельно от геометрии карты.
      </p>
    );
  }

  if (state.status === "error") {
    return (
      <p
        className="country-map-shell__alert"
        role="alert"
        aria-label="Ошибка данных событий"
      >
        Не удалось загрузить, проверить или связать данные событий. Готовая
        геометрия остается нейтральной.
      </p>
    );
  }

  const acceptedSnapshot = state.joinedSnapshot.snapshot;
  const secondaryParts = buildSecondaryStatusParts(state);
  return (
    <div className="country-map-shell__data-status">
      <p
        className="country-map-shell__status"
        role="status"
        aria-label="Состояние данных событий"
        aria-live="polite"
      >
        {`Принят снимок событий. UTC-период [${acceptedSnapshot.snapshot.from}, ${acceptedSnapshot.snapshot.to}). Время принятия: ${state.acceptedAt}. Сопоставлено событий: ${acceptedSnapshot.quality.mappedEventCount}. Полнота: ${acceptedSnapshot.coverage.status}.`}
      </p>
      {secondaryParts.length > 0 ? (
        <p
          className="country-map-shell__status country-map-shell__status--secondary"
          role="status"
          aria-label="Дополнительное состояние данных событий"
          aria-live="polite"
        >
          {secondaryParts.join(" ")}
        </p>
      ) : null}
      {state.refreshStatus === "warning" ||
      state.refreshStatus === "refreshing-with-warning" ? (
        <p
          className="country-map-shell__alert"
          role="alert"
          aria-label="Предупреждение обновления данных событий"
        >
          Не удалось обновить данные событий. Показан последний успешно принятый
          снимок.
        </p>
      ) : null}
    </div>
  );
}

function buildSecondaryStatusParts(
  state: Extract<CountrySnapshotRequestState, { status: "accepted" }>,
): string[] {
  const snapshot = state.joinedSnapshot.snapshot;
  const parts: string[] = [];

  if (snapshot.quality.unlocatedEventCount > 0) {
    parts.push(
      `Без надежного места действия: ${snapshot.quality.unlocatedEventCount}.`,
    );
  }
  if (snapshot.quality.unmappedEventCount > 0) {
    parts.push(
      `Не сопоставлено с границами: ${snapshot.quality.unmappedEventCount}.`,
    );
  }
  if (snapshot.coverage.status === "PARTIAL") {
    const intervals = snapshot.coverage.missingIntervals ?? [];
    parts.push(
      `Известные пропуски UTC: ${intervals
        .map((interval) => `[${interval.from}, ${interval.to})`)
        .join(", ")}.`,
    );
  } else if (snapshot.coverage.status === "UNKNOWN") {
    parts.push(
      "Полнота сейчас неизвестна; список пропусков недоступен и не подменяется пустым.",
    );
  }
  if (
    state.refreshStatus === "refreshing" ||
    state.refreshStatus === "refreshing-with-warning"
  ) {
    parts.push(
      "Загружается новый снимок; последние принятые данные остаются видимыми.",
    );
  }

  return parts;
}

function CountryToneLegend({
  mode,
}: Readonly<{ mode: CountryMapDisplayMode }>): JSX.Element {
  const items =
    mode === "tone"
      ? Object.values(COUNTRY_TONE_SUMMARIES)
      : COUNTRY_VOLUME_LEVELS.map((level) => ({
          ...level,
          description: "событий",
        }));
  return (
    <section
      className="country-map-shell__legend"
      aria-label={
        mode === "tone" ? "Шкала тональности" : "Шкала количества событий"
      }
    >
      <ul className="country-map-shell__tone-scale">
        {items.map((item) => (
          <li key={item.label}>
            <span
              className="country-map-shell__legend-swatch"
              style={{ backgroundColor: item.color }}
              aria-hidden="true"
            />
            <span>
              <strong>{item.label}</strong>
              <small>{item.description}</small>
            </span>
          </li>
        ))}
      </ul>
      <div className="country-map-shell__legend-notes">
        <span>
          <i style={{ backgroundColor: COUNTRY_EMPTY_COLOR }} />
          Нет событий в доступных данных
        </span>
        {mode === "tone" ? (
          <span>
            <i style={{ backgroundColor: COUNTRY_MISSING_TONE_COLOR }} />
            Тональность неизвестна
          </span>
        ) : null}
        <span>
          {mode === "tone"
            ? "Доли среди событий с известным тоном. Смешанная картина не означает нулевой тон."
            : "Все события, включая события с неизвестной тональностью."}
        </span>
      </div>
    </section>
  );
}

function CountryComposition({
  region,
}: Readonly<{ region: CountryVisualRegion }>): JSX.Element | null {
  if (region.status !== "tone-mixture") return null;
  const summary = COUNTRY_TONE_SUMMARIES[region.summaryKey];
  const parts = [
    {
      label: "Негативные",
      percentage: region.negativePercentage,
      color: "#bd3a2b",
    },
    { label: "Тон 0", percentage: region.zeroPercentage, color: "#929ba5" },
    {
      label: "Позитивные",
      percentage: region.positivePercentage,
      color: "#177f99",
    },
  ];
  return (
    <div
      className="country-map-shell__composition"
      role="group"
      aria-label="Состав тональности страны"
    >
      <p className="country-map-shell__composition-title">
        <span style={{ backgroundColor: summary.color }} aria-hidden="true" />
        {region.summaryKey === "zero"
          ? summary.label
          : `${summary.label} картина`}
      </p>
      <div className="country-map-shell__composition-bar" aria-hidden="true">
        {parts.map((part) => (
          <span
            key={part.label}
            style={{
              width: `${part.percentage}%`,
              backgroundColor: part.color,
            }}
          />
        ))}
      </div>
      <div className="country-map-shell__composition-values">
        {parts.map((part) => (
          <span key={part.label}>
            <strong>{formatPercentage(part.percentage)}</strong>
            {part.label}
          </span>
        ))}
      </div>
      {region.data.coloredEventCount < 30 ? (
        <p className="country-map-shell__small-sample">
          Мало событий с известным тоном: {region.data.coloredEventCount}.
          Учитывайте это при сравнении стран.
        </p>
      ) : null}
    </div>
  );
}

function CountryHoverSummary({
  region,
  coverageStatus,
}: Readonly<{
  region: CountryVisualRegion;
  coverageStatus: "COMPLETE" | "PARTIAL" | "UNKNOWN";
}>): JSX.Element {
  return (
    <aside
      className="country-map-shell__hover-card"
      role="tooltip"
      aria-label="Краткая сводка страны"
    >
      <strong>{region.geometry.properties.displayName}</strong>
      <p>{countryCountSummary(region, coverageStatus)}</p>
      <CountryComposition region={region} />
      {region.data.missingToneEventCount > 0 ? (
        <p>Тональность неизвестна: {region.data.missingToneEventCount}.</p>
      ) : null}
      {region.geometry.properties.disputeStatus === "DISPUTED_DE_FACTO" ? (
        <p>Спорная территория, показано представление de facto.</p>
      ) : null}
      {coverageStatus !== "COMPLETE" ? (
        <p>{`Предупреждение по всему снимку: ${coverageStatus}.`}</p>
      ) : null}
    </aside>
  );
}

function CountrySelectedSummary({
  region,
  state,
  onClose,
}: Readonly<{
  region: CountryVisualRegion;
  state: Extract<CountrySnapshotRequestState, { status: "accepted" }>;
  onClose: () => void;
}>): JSX.Element {
  const snapshot = state.joinedSnapshot.snapshot;
  return (
    <aside
      className="country-map-shell__country-card"
      aria-labelledby="selected-country-title"
    >
      <div className="country-map-shell__country-card-heading">
        <div>
          <p>Закрепленная страна</p>
          <h2 id="selected-country-title">
            {region.geometry.properties.displayName}
          </h2>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Закрыть сводку страны"
        >
          Закрыть
        </button>
      </div>

      <p>{countryCountSummary(region, snapshot.coverage.status)}</p>
      <p>
        С известной тональностью: {region.data.coloredEventCount}. Тональность
        неизвестна: {region.data.missingToneEventCount}.
      </p>
      <CountryComposition region={region} />
      <ToneGroupSummary groups={region.groups} />
      <details className="country-map-shell__metadata">
        <summary>Период, полнота и границы</summary>
        <p>{`UTC-период [${snapshot.snapshot.from}, ${snapshot.snapshot.to}).`}</p>
        <p>{`Полнота всего снимка: ${snapshot.coverage.status}.`}</p>
        <p>
          Источник геометрии: Natural Earth 10m (
          {region.geometry.properties.geometrySource}), версия country-v1.
        </p>
        <p>
          {region.geometry.properties.disputeStatus === "DISPUTED_DE_FACTO"
            ? "Статус: спорная территория в представлении de facto."
            : "Статус: стандартная территория в выбранной геометрии."}
        </p>
      </details>
    </aside>
  );
}

function ToneGroupSummary({
  groups,
}: Readonly<{
  groups: readonly CountryToneVisualGroup[];
}>): JSX.Element | null {
  if (groups.every((group) => group.count === 0)) return null;
  return (
    <section
      className="country-map-shell__distribution"
      aria-label="Все группы тональности страны"
    >
      <h3>Распределение по семи диапазонам</h3>
      <ul className="country-map-shell__tone-summary">
        {groups.map((group) => (
          <li key={group.key}>
            <div className="country-map-shell__tone-row">
              <span>{group.label}</span>
              <span>
                {group.count}{" "}
                <strong>{formatPercentage(group.percentage)}</strong>
              </span>
            </div>
            <div className="country-map-shell__tone-track" aria-hidden="true">
              <span
                style={{
                  width: `${group.percentage}%`,
                  backgroundColor: group.color,
                }}
              />
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}

function countryCountSummary(
  region: CountryVisualRegion,
  coverageStatus: "COMPLETE" | "PARTIAL" | "UNKNOWN",
): string {
  if (region.status === "no-events") {
    return coverageStatus === "COMPLETE"
      ? "0 событий."
      : "0 в доступных данных.";
  }
  if (region.status === "missing-tone-only") {
    return `Событий: ${region.data.eventCount}. Для всех событий тональность неизвестна.`;
  }
  return `Событий: ${region.data.eventCount}.`;
}

function formatPercentage(percentage: number): string {
  return `${Number.isInteger(percentage) ? percentage.toString() : percentage.toFixed(1)}%`;
}

function resolveTechnicalState(
  mapState: MapLibreTechnicalState,
  geometryStatus: "loading" | "loaded" | "error",
): CountryMapTechnicalState {
  if (mapState.status === "provider-error") {
    return mapState;
  }

  if (geometryStatus === "error") {
    return { status: "geometry-error" };
  }

  return mapState;
}
