import { useRef } from "react";
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
        <p className="country-map-shell__eyebrow">Event Mosaic</p>
        <h1 id="country-map-title">Карта событий по странам</h1>
        <p className="country-map-shell__intro">
          Экран связывает проверенные границы стран с последним проверенным
          снимком событий GDELT.
        </p>
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
  const geometryState = useCountryGeometry({ loader: geometryLoader });
  const snapshotState = useCountrySnapshot({
    geometry: geometryState.status === "loaded" ? geometryState.geometry : null,
    loader: snapshotLoader,
    clock: snapshotClock,
    refreshIntervalMilliseconds: snapshotRefreshIntervalMilliseconds,
  });
  const mapState = useMapLibreMap({
    containerRef,
    style: configuration.styleUrl,
    attributionControl: configuration.attributionControl,
    geometry: geometryState.status === "loaded" ? geometryState.geometry : null,
    mapFactory,
  });
  const technicalState = resolveTechnicalState(mapState, geometryState.status);

  return (
    <section
      className="country-map-shell__map-region"
      aria-label="Область карты стран"
      aria-busy={technicalState.status === "loading"}
    >
      <div
        ref={containerRef}
        className="country-map-shell__canvas"
        role="region"
        aria-label="Фоновая карта мира"
      />
      <MapTechnicalStatus state={technicalState} />
      <SnapshotTechnicalStatus state={snapshotState} />
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
          className="country-map-shell__status"
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
          className="country-map-shell__status"
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
  return (
    <>
      <p
        className="country-map-shell__status"
        role="status"
        aria-label="Состояние данных событий"
        aria-live="polite"
      >
        {`Принят снимок событий. UTC-период [${acceptedSnapshot.snapshot.from}, ${acceptedSnapshot.snapshot.to}). Время принятия: ${state.acceptedAt}. Сопоставлено событий: ${acceptedSnapshot.quality.mappedEventCount}. Полнота: ${acceptedSnapshot.coverage.status}.`}
      </p>
      {state.refreshStatus === "refreshing" ||
      state.refreshStatus === "refreshing-with-warning" ? (
        <p
          className="country-map-shell__status"
          role="status"
          aria-label="Состояние обновления данных событий"
          aria-live="polite"
        >
          Загружается новый снимок. Последние принятые данные остаются
          доступными.
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
    </>
  );
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
