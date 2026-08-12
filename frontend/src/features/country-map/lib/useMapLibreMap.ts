import { Map as MapLibreMap } from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import type { AddLayerObject, MapOptions } from "maplibre-gl";
import type { RefObject } from "react";

import type {
  CountryGeometry,
  CountryMultiPolygonCoordinates,
  CountryPolygonCoordinates,
  CountryPosition,
} from "../api/countryGeometry";

export type MapLibreStyle = NonNullable<MapOptions["style"]>;

export type MapLibreTechnicalState =
  | Readonly<{ status: "loading" }>
  | Readonly<{ status: "ready" }>
  | Readonly<{ status: "provider-error" }>;

export type MapLibreOwnedEvent = "load" | "error";

export interface MapLibreOwnedSubscription {
  unsubscribe(): void;
}

export interface MapLibreOwnedMap {
  subscribe(
    event: MapLibreOwnedEvent,
    listener: () => void,
  ): MapLibreOwnedSubscription;
  hasSource(id: string): boolean;
  addCountryGeometrySource(id: string, geometry: CountryGeometry): void;
  hasLayer(id: string): boolean;
  addLayer(layer: AddLayerObject): void;
  remove(): void;
}

export type MapLibreMapFactory = (options: MapOptions) => MapLibreOwnedMap;

type UseMapLibreMapOptions = Readonly<{
  containerRef: RefObject<HTMLDivElement | null>;
  style: MapLibreStyle;
  attributionControl: Readonly<{
    compact: false;
  }>;
  geometry: CountryGeometry | null;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

export const COUNTRY_GEOMETRY_SOURCE_ID = "event-mosaic-country-geometry";
export const COUNTRY_FILL_LAYER_ID = "event-mosaic-country-fill";
export const COUNTRY_LINE_LAYER_ID = "event-mosaic-country-line";

const COUNTRY_FILL_LAYER = {
  id: COUNTRY_FILL_LAYER_ID,
  type: "fill",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "fill-color": "#4f7175",
    "fill-opacity": 0.28,
  },
} as const satisfies AddLayerObject;

const COUNTRY_LINE_LAYER = {
  id: COUNTRY_LINE_LAYER_ID,
  type: "line",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "line-color": "#2f494d",
    "line-width": 0.8,
  },
} as const satisfies AddLayerObject;

const LOADING_STATE = {
  status: "loading",
} as const satisfies MapLibreTechnicalState;

const PROVIDER_ERROR_STATE = {
  status: "provider-error",
} as const satisfies MapLibreTechnicalState;

const READY_STATE = {
  status: "ready",
} as const satisfies MapLibreTechnicalState;

/**
 * Создает одну карту, владеет ее начальными событиями и полностью освобождает
 * MapLibre при удалении React-компонента.
 */
export function useMapLibreMap({
  containerRef,
  style,
  attributionControl,
  geometry,
  mapFactory = createMapLibreMap,
}: UseMapLibreMapOptions): MapLibreTechnicalState {
  const mapRef = useRef<MapLibreOwnedMap | null>(null);
  const geometryRef = useRef<CountryGeometry | null>(geometry);
  const isStyleLoadedRef = useRef(false);
  const attributionCompact = attributionControl.compact;
  const [technicalState, setTechnicalState] =
    useState<MapLibreTechnicalState>(LOADING_STATE);

  useEffect(() => {
    const container = containerRef.current;

    if (container === null) {
      return undefined;
    }

    let isOwned = true;
    isStyleLoadedRef.current = false;
    const map = mapFactory({
      container,
      style,
      center: [0, 20],
      zoom: 1,
      attributionControl: {
        compact: attributionCompact,
      },
    });
    mapRef.current = map;

    const loadSubscription = map.subscribe("load", () => {
      if (isOwned) {
        isStyleLoadedRef.current = true;
        const currentGeometry = geometryRef.current;

        setTechnicalState(
          currentGeometry === null || !syncCountryLayer(map, currentGeometry)
            ? LOADING_STATE
            : READY_STATE,
        );
      }
    });
    const errorSubscription = map.subscribe("error", () => {
      if (isOwned && !isStyleLoadedRef.current) {
        setTechnicalState(PROVIDER_ERROR_STATE);
      }
    });

    return () => {
      isOwned = false;
      loadSubscription.unsubscribe();
      errorSubscription.unsubscribe();
      map.remove();

      if (mapRef.current === map) {
        mapRef.current = null;
        isStyleLoadedRef.current = false;
      }
    };
  }, [attributionCompact, containerRef, mapFactory, style]);

  useEffect(() => {
    geometryRef.current = geometry;
    const map = mapRef.current;

    if (
      geometry !== null &&
      map !== null &&
      isStyleLoadedRef.current &&
      syncCountryLayer(map, geometry)
    ) {
      setTechnicalState(READY_STATE);
    }
  }, [geometry]);

  return technicalState;
}

function syncCountryLayer(
  map: MapLibreOwnedMap,
  geometry: CountryGeometry,
): boolean {
  if (!map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID)) {
    map.addCountryGeometrySource(COUNTRY_GEOMETRY_SOURCE_ID, geometry);
  }

  if (!map.hasLayer(COUNTRY_FILL_LAYER_ID)) {
    map.addLayer(COUNTRY_FILL_LAYER);
  }

  if (!map.hasLayer(COUNTRY_LINE_LAYER_ID)) {
    map.addLayer(COUNTRY_LINE_LAYER);
  }

  return (
    map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID) &&
    map.hasLayer(COUNTRY_FILL_LAYER_ID) &&
    map.hasLayer(COUNTRY_LINE_LAYER_ID)
  );
}

function createMapLibreMap(options: MapOptions): MapLibreOwnedMap {
  const map = new MapLibreMap(options);

  return {
    subscribe(event, listener) {
      if (event === "load") {
        return map.on("load", listener);
      }

      return map.on("error", listener);
    },
    hasSource(id) {
      return map.getSource(id) !== undefined;
    },
    addCountryGeometrySource(id, geometry) {
      map.addSource(id, {
        type: "geojson",
        data: createMapLibreFeatureCollection(geometry),
      });
    },
    hasLayer(id) {
      return map.getLayer(id) !== undefined;
    },
    addLayer(layer) {
      map.addLayer(layer);
    },
    remove() {
      map.remove();
    },
  };
}

function createMapLibreFeatureCollection(geometry: CountryGeometry) {
  return {
    type: "FeatureCollection" as const,
    features: geometry.features.map((feature) => ({
      type: "Feature" as const,
      properties: {
        regionId: feature.properties.regionId,
        displayName: feature.properties.displayName,
        disputeStatus: feature.properties.disputeStatus,
        geometrySource: feature.properties.geometrySource,
      },
      geometry:
        feature.geometry.type === "Polygon"
          ? {
              type: "Polygon" as const,
              coordinates: copyPolygonCoordinates(feature.geometry.coordinates),
            }
          : {
              type: "MultiPolygon" as const,
              coordinates: copyMultiPolygonCoordinates(
                feature.geometry.coordinates,
              ),
            },
    })),
  };
}

function copyMultiPolygonCoordinates(
  coordinates: CountryMultiPolygonCoordinates,
): [number, number][][][] {
  return coordinates.map(copyPolygonCoordinates);
}

function copyPolygonCoordinates(
  coordinates: CountryPolygonCoordinates,
): [number, number][][] {
  return coordinates.map((ring) => ring.map(copyPosition));
}

function copyPosition(position: CountryPosition): [number, number] {
  return [position[0], position[1]];
}
