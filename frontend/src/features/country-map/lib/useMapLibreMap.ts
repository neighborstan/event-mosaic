import { Map as MapLibreMap } from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import type {
  AddLayerObject,
  MapGeoJSONFeature,
  MapOptions,
} from "maplibre-gl";
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

export type CountryMapRegionVisual = Readonly<{
  regionId: string;
  status: "no-events" | "missing-tone-only" | "tone-mixture";
  fillColor: string;
}>;

export type MapLibreOwnedEvent = "load" | "style.load" | "error";
export type MapLibreCountryEvent = "hover" | "leave" | "select";

export interface MapLibreOwnedSubscription {
  unsubscribe(): void;
}

export interface MapLibreOwnedMap {
  subscribe(
    event: MapLibreOwnedEvent,
    listener: () => void,
  ): MapLibreOwnedSubscription;
  subscribeCountry(
    event: MapLibreCountryEvent,
    layerId: string,
    listener: (regionId: string | null) => void,
  ): MapLibreOwnedSubscription;
  subscribeBackgroundSelect(listener: () => void): MapLibreOwnedSubscription;
  hasSource(id: string): boolean;
  addCountryGeometrySource(
    id: string,
    geometry: CountryGeometry,
    promoteId: "regionId",
  ): void;
  hasLayer(id: string): boolean;
  addLayer(layer: AddLayerObject): void;
  setCountryFeatureState(
    regionId: string,
    state: Readonly<Record<string, string | boolean>>,
  ): void;
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
  visualRegions: readonly CountryMapRegionVisual[] | null;
  hoveredRegionId: string | null;
  selectedRegionId: string | null;
  onHoverRegion: (regionId: string | null) => void;
  onSelectRegion: (regionId: string | null) => void;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

export const COUNTRY_GEOMETRY_SOURCE_ID = "event-mosaic-country-geometry";
export const COUNTRY_FILL_LAYER_ID = "event-mosaic-country-fill";
export const COUNTRY_LINE_LAYER_ID = "event-mosaic-country-line";
export const COUNTRY_STATE_LAYER_ID = "event-mosaic-country-state";

const COUNTRY_FILL_LAYER = {
  id: COUNTRY_FILL_LAYER_ID,
  type: "fill",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "fill-color": ["coalesce", ["feature-state", "fillColor"], "#dfe7e8"],
    "fill-opacity": 1,
  },
} as const satisfies AddLayerObject;

const COUNTRY_LINE_LAYER = {
  id: COUNTRY_LINE_LAYER_ID,
  type: "line",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "line-color": [
      "case",
      ["==", ["get", "disputeStatus"], "DISPUTED_DE_FACTO"],
      "#985268",
      "#2f494d",
    ],
    "line-width": [
      "case",
      ["==", ["get", "disputeStatus"], "DISPUTED_DE_FACTO"],
      1.35,
      0.8,
    ],
  },
} as const satisfies AddLayerObject;

const COUNTRY_STATE_LAYER = {
  id: COUNTRY_STATE_LAYER_ID,
  type: "line",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "line-width": [
      "case",
      ["boolean", ["feature-state", "selected"], false],
      2.5,
      ["boolean", ["feature-state", "hovered"], false],
      1.8,
      0,
    ],
    "line-color": [
      "case",
      ["boolean", ["feature-state", "selected"], false],
      "#b36b16",
      ["boolean", ["feature-state", "hovered"], false],
      "#e7a638",
      "rgba(0, 0, 0, 0)",
    ],
  },
} as const satisfies AddLayerObject;

const OWNED_LAYERS = [
  COUNTRY_FILL_LAYER,
  COUNTRY_LINE_LAYER,
  COUNTRY_STATE_LAYER,
] as const;

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
 * Создает одну карту и в одном месте владеет геометрией, сводной окраской,
 * feature state и смысловыми событиями стран.
 */
export function useMapLibreMap({
  containerRef,
  style,
  attributionControl,
  geometry,
  visualRegions,
  hoveredRegionId,
  selectedRegionId,
  onHoverRegion,
  onSelectRegion,
  mapFactory = createMapLibreMap,
}: UseMapLibreMapOptions): MapLibreTechnicalState {
  const mapRef = useRef<MapLibreOwnedMap | null>(null);
  const geometryRef = useRef<CountryGeometry | null>(geometry);
  const visualRegionsRef = useRef<readonly CountryMapRegionVisual[] | null>(
    visualRegions,
  );
  const hoveredRegionIdRef = useRef<string | null>(hoveredRegionId);
  const selectedRegionIdRef = useRef<string | null>(selectedRegionId);
  const onHoverRegionRef = useRef(onHoverRegion);
  const onSelectRegionRef = useRef(onSelectRegion);
  const isStyleLoadedRef = useRef(false);
  const attributionCompact = attributionControl.compact;
  const [technicalState, setTechnicalState] =
    useState<MapLibreTechnicalState>(LOADING_STATE);

  useEffect(() => {
    geometryRef.current = geometry;
    visualRegionsRef.current = visualRegions;
    onHoverRegionRef.current = onHoverRegion;
    onSelectRegionRef.current = onSelectRegion;
  }, [geometry, onHoverRegion, onSelectRegion, visualRegions]);

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

    const handleStyleReady = (): void => {
      if (!isOwned) {
        return;
      }

      isStyleLoadedRef.current = true;
      const currentGeometry = geometryRef.current;
      setTechnicalState(
        currentGeometry === null ||
          !syncCountryResources(
            map,
            currentGeometry,
            visualRegionsRef.current,
            hoveredRegionIdRef.current,
            selectedRegionIdRef.current,
          )
          ? LOADING_STATE
          : READY_STATE,
      );
    };

    const loadSubscription = map.subscribe("load", handleStyleReady);
    const styleLoadSubscription = map.subscribe("style.load", handleStyleReady);
    const errorSubscription = map.subscribe("error", () => {
      if (isOwned && !isStyleLoadedRef.current) {
        setTechnicalState(PROVIDER_ERROR_STATE);
      }
    });
    const hoverSubscription = map.subscribeCountry(
      "hover",
      COUNTRY_FILL_LAYER_ID,
      (regionId) => {
        if (isOwned) {
          onHoverRegionRef.current(regionId);
        }
      },
    );
    const leaveSubscription = map.subscribeCountry(
      "leave",
      COUNTRY_FILL_LAYER_ID,
      () => {
        if (isOwned) {
          onHoverRegionRef.current(null);
        }
      },
    );
    const selectSubscription = map.subscribeCountry(
      "select",
      COUNTRY_FILL_LAYER_ID,
      (regionId) => {
        if (isOwned && regionId !== null) {
          onSelectRegionRef.current(regionId);
        }
      },
    );
    const backgroundSubscription = map.subscribeBackgroundSelect(() => {
      if (isOwned) {
        onSelectRegionRef.current(null);
      }
    });

    return () => {
      isOwned = false;
      loadSubscription.unsubscribe();
      styleLoadSubscription.unsubscribe();
      errorSubscription.unsubscribe();
      hoverSubscription.unsubscribe();
      leaveSubscription.unsubscribe();
      selectSubscription.unsubscribe();
      backgroundSubscription.unsubscribe();
      map.remove();

      if (mapRef.current === map) {
        mapRef.current = null;
        isStyleLoadedRef.current = false;
      }
    };
  }, [attributionCompact, containerRef, mapFactory, style]);

  useEffect(() => {
    const map = mapRef.current;
    if (
      geometry !== null &&
      map !== null &&
      isStyleLoadedRef.current &&
      syncCountryResources(
        map,
        geometry,
        visualRegions,
        hoveredRegionIdRef.current,
        selectedRegionIdRef.current,
      )
    ) {
      setTechnicalState(READY_STATE);
    }
  }, [geometry, visualRegions]);

  useEffect(() => {
    const map = mapRef.current;
    const previousRegionId = hoveredRegionIdRef.current;
    hoveredRegionIdRef.current = hoveredRegionId;

    if (map !== null && isStyleLoadedRef.current) {
      updateInteractionState(map, previousRegionId, hoveredRegionId, "hovered");
    }
  }, [hoveredRegionId]);

  useEffect(() => {
    const map = mapRef.current;
    const previousRegionId = selectedRegionIdRef.current;
    selectedRegionIdRef.current = selectedRegionId;

    if (map !== null && isStyleLoadedRef.current) {
      updateInteractionState(
        map,
        previousRegionId,
        selectedRegionId,
        "selected",
      );
    }
  }, [selectedRegionId]);

  return technicalState;
}

function syncCountryResources(
  map: MapLibreOwnedMap,
  geometry: CountryGeometry,
  visualRegions: readonly CountryMapRegionVisual[] | null,
  hoveredRegionId: string | null,
  selectedRegionId: string | null,
): boolean {
  if (!map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID)) {
    map.addCountryGeometrySource(
      COUNTRY_GEOMETRY_SOURCE_ID,
      geometry,
      "regionId",
    );
  }

  for (const layer of OWNED_LAYERS) {
    if (!map.hasLayer(layer.id)) map.addLayer(layer);
  }
  if (visualRegions !== null) {
    for (const region of visualRegions) {
      map.setCountryFeatureState(region.regionId, {
        visualStatus: region.status,
        fillColor: region.fillColor,
        hovered: region.regionId === hoveredRegionId,
        selected: region.regionId === selectedRegionId,
      });
    }
  }
  return (
    map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID) &&
    OWNED_LAYERS.every((layer) => map.hasLayer(layer.id))
  );
}

function updateInteractionState(
  map: MapLibreOwnedMap,
  previousRegionId: string | null,
  nextRegionId: string | null,
  key: "hovered" | "selected",
): void {
  if (previousRegionId !== null && previousRegionId !== nextRegionId) {
    map.setCountryFeatureState(previousRegionId, { [key]: false });
  }
  if (nextRegionId !== null) {
    map.setCountryFeatureState(nextRegionId, { [key]: true });
  }
}

function createMapLibreMap(options: MapOptions): MapLibreOwnedMap {
  const map = new MapLibreMap(options);

  return {
    subscribe(event, listener) {
      return map.on(event, listener);
    },
    subscribeCountry(event, layerId, listener) {
      if (event === "hover") {
        return map.on("mousemove", layerId, (mapEvent) => {
          listener(readRegionId(mapEvent.features));
        });
      }
      if (event === "leave") {
        return map.on("mouseleave", layerId, () => {
          listener(null);
        });
      }

      return map.on("click", layerId, (mapEvent) => {
        listener(readRegionId(mapEvent.features));
      });
    },
    subscribeBackgroundSelect(listener) {
      return map.on("click", (mapEvent) => {
        const countries = map.queryRenderedFeatures(mapEvent.point, {
          layers: [COUNTRY_FILL_LAYER_ID],
        });
        if (countries.length === 0) {
          listener();
        }
      });
    },
    hasSource(id) {
      return map.getSource(id) !== undefined;
    },
    addCountryGeometrySource(id, countryGeometry, promoteId) {
      map.addSource(id, {
        type: "geojson",
        data: createMapLibreFeatureCollection(countryGeometry),
        promoteId,
      });
    },
    hasLayer(id) {
      return map.getLayer(id) !== undefined;
    },
    addLayer(layer) {
      // Подписи фоновой карты остаются над сводной окраской и границами.
      const styleLayers = map.getStyle().layers;
      const firstLabel = styleLayers.find(
        (candidate) => candidate.type === "symbol",
      )?.id;
      map.addLayer(layer, firstLabel);
      if (layer.id === COUNTRY_FILL_LAYER_ID) {
        // Фон страны теперь задает сводка, поэтому подписи обоих providers
        // должны читаться на каждом из ее оттенков.
        for (const label of styleLayers) {
          if (
            label.type === "symbol" &&
            label.layout?.["text-field"] !== undefined
          ) {
            map.setPaintProperty(label.id, "text-color", "#24383e");
            map.setPaintProperty(label.id, "text-halo-color", "#ffffff");
            map.setPaintProperty(label.id, "text-halo-width", 1.2);
          }
        }
      }
    },
    setCountryFeatureState(regionId, state) {
      map.setFeatureState(
        {
          source: COUNTRY_GEOMETRY_SOURCE_ID,
          id: regionId,
        },
        state,
      );
    },
    remove() {
      map.remove();
    },
  };
}

function readRegionId(
  features: readonly MapGeoJSONFeature[] | undefined,
): string | null {
  const feature = features?.[0];
  if (typeof feature?.id === "string") {
    return feature.id;
  }

  const properties: unknown = feature?.properties;
  if (!isUnknownRecord(properties)) {
    return null;
  }

  const propertyRegionId = properties["regionId"];
  return typeof propertyRegionId === "string" ? propertyRegionId : null;
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
