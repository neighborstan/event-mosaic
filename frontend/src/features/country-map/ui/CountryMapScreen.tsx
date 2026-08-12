import { useRef } from "react";
import type { JSX } from "react";

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
  mapFactory?: MapLibreMapFactory | undefined;
}>;

type CountryMapCanvasProps = Readonly<{
  configuration: BasemapConfiguration;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

export function CountryMapScreen({
  providerValue = import.meta.env.VITE_MAP_BASEMAP_PROVIDER,
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
  mapFactory,
}: CountryMapCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const technicalState = useMapLibreMap({
    containerRef,
    style: configuration.styleUrl,
    attributionControl: configuration.attributionControl,
    mapFactory,
  });

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
}: Readonly<{ state: MapLibreTechnicalState }>): JSX.Element {
  switch (state.status) {
    case "loading":
      return (
        <p
          className="country-map-shell__status"
          role="status"
          aria-live="polite"
        >
          Фоновая карта загружается. Данные по странам пока не подключены.
        </p>
      );
    case "provider-error":
      return (
        <p className="country-map-shell__alert" role="alert">
          Не удалось загрузить выбранную фоновую карту. Автоматическое
          переключение источника не выполняется.
        </p>
      );
  }
}
