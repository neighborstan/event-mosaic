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
  COUNTRY_TONE_LEGEND_ITEMS,
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
            Цвет показывает фактическую тональность событий, а плотность - их
            объем за последний проверенный период.
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
      joinedSnapshot === null ? null : buildCountryVisualModel(joinedSnapshot),
    [joinedSnapshot],
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
      {snapshotState.status === "accepted" ? <CountryToneLegend /> : null}
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

function CountryToneLegend(): JSX.Element {
  return (
    <section
      className="country-map-shell__legend"
      aria-labelledby="country-map-legend-title"
    >
      <div className="country-map-shell__legend-heading">
        <h2 id="country-map-legend-title">Легенда тональности</h2>
        <p>
          Цветовые доли визуально усилены квадратным корнем, чтобы меньшая
          группа оставалась заметной. Подсказка и карточка показывают только
          фактические проценты.
        </p>
      </div>
      <ul
        className="country-map-shell__tone-scale"
        aria-label="Семь фактических групп тональности"
      >
        {COUNTRY_TONE_LEGEND_ITEMS.map((item) => (
          <li key={item.key}>
            <span
              className="country-map-shell__legend-swatch"
              style={{ backgroundColor: item.color }}
              aria-hidden="true"
            />
            <span>
              <strong>{item.label}</strong>
              <small>{item.range}</small>
            </span>
          </li>
        ))}
      </ul>
      <ul
        className="country-map-shell__special-states"
        aria-label="Плотность и специальные состояния"
      >
        <li>
          <span
            className="country-map-shell__legend-swatch country-map-shell__legend-swatch--density"
            aria-hidden="true"
          />
          Плотность и прозрачность растут с числом событий с известной
          тональностью и имеют верхний предел.
        </li>
        <li>
          <span
            className="country-map-shell__legend-swatch country-map-shell__legend-swatch--empty"
            aria-hidden="true"
          />
          "0 событий" - отдельное неокрашенное состояние.
        </li>
        <li>
          <span
            className="country-map-shell__legend-swatch country-map-shell__legend-swatch--missing"
            aria-hidden="true"
          />
          "Тональность неизвестна" не считается настоящим тоном 0.
        </li>
      </ul>
    </section>
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
      <ToneGroupSummary groups={region.groups} compact />
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
      <p>{`UTC-период [${snapshot.snapshot.from}, ${snapshot.snapshot.to}).`}</p>
      <p>{countryCountSummary(region, snapshot.coverage.status)}</p>
      <p>
        С известной тональностью: {region.data.coloredEventCount}. Тональность
        неизвестна: {region.data.missingToneEventCount}.
      </p>
      <ToneGroupSummary groups={region.groups} compact={false} />
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
    </aside>
  );
}

function ToneGroupSummary({
  groups,
  compact,
}: Readonly<{
  groups: readonly CountryToneVisualGroup[];
  compact: boolean;
}>): JSX.Element | null {
  const visibleGroups = compact
    ? groups.filter((group) => group.count > 0)
    : groups;
  if (visibleGroups.length === 0) {
    return null;
  }

  return (
    <ul
      className="country-map-shell__tone-summary"
      aria-label={
        compact
          ? "Ненулевые группы тональности страны"
          : "Все группы тональности страны"
      }
    >
      {visibleGroups.map((group) => (
        <li key={group.key}>
          <span
            className="country-map-shell__legend-swatch"
            style={{ backgroundColor: group.color }}
            aria-hidden="true"
          />
          <span>{`${group.label}: ${group.count} (${formatPercentage(group.percentage)}).`}</span>
        </li>
      ))}
    </ul>
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
