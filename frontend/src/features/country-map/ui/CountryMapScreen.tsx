import { useRef } from "react";
import type { JSX } from "react";

import {
  useCountryGeometry,
  type CountryGeometryLoader,
} from "../api/useCountryGeometry";
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
  mapFactory?: MapLibreMapFactory | undefined;
}>;

type CountryMapCanvasProps = Readonly<{
  configuration: BasemapConfiguration;
  geometryLoader?: CountryGeometryLoader | undefined;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

type CountryMapTechnicalState =
  MapLibreTechnicalState | Readonly<{ status: "geometry-error" }>;

export function CountryMapScreen({
  providerValue = import.meta.env.VITE_MAP_BASEMAP_PROVIDER,
  geometryLoader,
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
          Первый экран готовит отдельную и проверяемую основу карты событий
          GDELT.
        </p>
        {providerResolution.status === "resolved" ? (
          <CountryMapCanvas
            configuration={providerResolution.configuration}
            geometryLoader={geometryLoader}
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
  mapFactory,
}: CountryMapCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const geometryState = useCountryGeometry({ loader: geometryLoader });
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
          aria-live="polite"
        >
          Карта готова. Показаны границы 258 стран и территорий.
        </p>
      );
    case "provider-error":
      return (
        <p className="country-map-shell__alert" role="alert">
          Не удалось загрузить выбранную фоновую карту. Автоматическое
          переключение источника не выполняется.
        </p>
      );
    case "geometry-error":
      return (
        <p className="country-map-shell__alert" role="alert">
          Не удалось загрузить или проверить геометрию стран. Поврежденные
          данные не отображаются как пустая карта.
        </p>
      );
  }
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
