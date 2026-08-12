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

type CountryMapRegionVisualBase = Readonly<{
  regionId: string;
}>;

export type CountryMapRegionVisual =
  | (CountryMapRegionVisualBase & Readonly<{ status: "no-events" }>)
  | (CountryMapRegionVisualBase & Readonly<{ status: "missing-tone-only" }>)
  | (CountryMapRegionVisualBase &
      Readonly<{
        status: "tone-mixture";
        pattern: Readonly<{
          fingerprint: string;
          rgba: Uint8Array | Uint8ClampedArray;
          width: number;
          height: number;
        }>;
      }>);

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
  hasImage(id: string): boolean;
  addImage(
    id: string,
    image: Readonly<{
      width: number;
      height: number;
      data: Uint8Array | Uint8ClampedArray;
    }>,
  ): void;
  removeImage(id: string): void;
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
export const COUNTRY_TONE_LAYER_ID = "event-mosaic-country-tone";
export const COUNTRY_LINE_LAYER_ID = "event-mosaic-country-line";
export const COUNTRY_STATE_LAYER_ID = "event-mosaic-country-state";

const TRANSPARENT_PATTERN_ID = "event-mosaic-country-pattern-transparent";
const TONE_PATTERN_PREFIX = "event-mosaic-country-pattern:";

const COUNTRY_FILL_LAYER = {
  id: COUNTRY_FILL_LAYER_ID,
  type: "fill",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "fill-color": [
      "case",
      ["==", ["feature-state", "visualStatus"], "no-events"],
      "#dfe7e8",
      ["==", ["feature-state", "visualStatus"], "missing-tone-only"],
      "#746d7d",
      ["==", ["feature-state", "visualStatus"], "tone-mixture"],
      "#eef3f3",
      "#4f7175",
    ],
    "fill-opacity": [
      "case",
      ["==", ["feature-state", "visualStatus"], "no-events"],
      0.3,
      ["==", ["feature-state", "visualStatus"], "missing-tone-only"],
      0.46,
      ["==", ["feature-state", "visualStatus"], "tone-mixture"],
      0.12,
      0.28,
    ],
  },
} as const satisfies AddLayerObject;

const COUNTRY_TONE_LAYER = {
  id: COUNTRY_TONE_LAYER_ID,
  type: "fill",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "fill-pattern": [
      "image",
      ["coalesce", ["feature-state", "patternImageId"], TRANSPARENT_PATTERN_ID],
    ],
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
  type: "fill",
  source: COUNTRY_GEOMETRY_SOURCE_ID,
  paint: {
    "fill-color": "#f5c65f",
    "fill-opacity": [
      "case",
      ["boolean", ["feature-state", "selected"], false],
      0.24,
      ["boolean", ["feature-state", "hovered"], false],
      0.1,
      0,
    ],
    "fill-outline-color": [
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
  COUNTRY_TONE_LAYER,
  COUNTRY_LINE_LAYER,
  COUNTRY_STATE_LAYER,
] as const;

const TRANSPARENT_PATTERN = {
  width: 1,
  height: 1,
  data: new Uint8ClampedArray(4),
} as const;

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
 * Создает одну карту и в одном месте владеет геометрией, динамическими
 * изображениями, feature state и смысловыми событиями стран.
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
  const ownedToneImageByRegionRef = useRef(new Map<string, string>());
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
    ownedToneImageByRegionRef.current = new Map<string, string>();
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
            ownedToneImageByRegionRef.current,
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
        ownedToneImageByRegionRef.current = new Map<string, string>();
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
        ownedToneImageByRegionRef.current,
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
  ownedToneImageByRegion: Map<string, string>,
): boolean {
  if (!map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID)) {
    map.addCountryGeometrySource(
      COUNTRY_GEOMETRY_SOURCE_ID,
      geometry,
      "regionId",
    );
  }

  if (!map.hasImage(TRANSPARENT_PATTERN_ID)) {
    map.addImage(TRANSPARENT_PATTERN_ID, TRANSPARENT_PATTERN);
  }

  for (const layer of OWNED_LAYERS) {
    if (!map.hasLayer(layer.id)) {
      map.addLayer(layer);
    }
  }

  const nextToneImageByRegion = new Map<string, string>();
  const nextToneImages = new Map<
    string,
    Readonly<{
      width: number;
      height: number;
      data: Uint8Array | Uint8ClampedArray;
    }>
  >();
  if (visualRegions !== null) {
    for (const region of visualRegions) {
      if (region.status !== "tone-mixture") {
        continue;
      }

      const imageId = patternImageId(region.pattern.fingerprint);
      nextToneImageByRegion.set(region.regionId, imageId);
      nextToneImages.set(imageId, {
        width: region.pattern.width,
        height: region.pattern.height,
        data: region.pattern.rgba,
      });
    }
  }

  const nextToneImageIds = new Set(nextToneImages.keys());
  const staleToneImageIds = new Set<string>();
  for (const imageId of ownedToneImageByRegion.values()) {
    if (!nextToneImageIds.has(imageId)) {
      staleToneImageIds.add(imageId);
    }
  }

  for (const [regionId, imageId] of ownedToneImageByRegion) {
    if (staleToneImageIds.has(imageId)) {
      map.setCountryFeatureState(regionId, {
        patternImageId: TRANSPARENT_PATTERN_ID,
      });
    }
  }

  for (const staleImageId of staleToneImageIds) {
    if (map.hasImage(staleImageId)) {
      map.removeImage(staleImageId);
    }
  }

  for (const [imageId, image] of nextToneImages) {
    if (!map.hasImage(imageId)) {
      map.addImage(imageId, image);
    }
  }

  if (visualRegions !== null) {
    for (const region of visualRegions) {
      map.setCountryFeatureState(region.regionId, {
        visualStatus: region.status,
        patternImageId:
          region.status === "tone-mixture"
            ? patternImageId(region.pattern.fingerprint)
            : TRANSPARENT_PATTERN_ID,
        hovered: region.regionId === hoveredRegionId,
        selected: region.regionId === selectedRegionId,
      });
    }
  }

  ownedToneImageByRegion.clear();
  for (const [regionId, imageId] of nextToneImageByRegion) {
    ownedToneImageByRegion.set(regionId, imageId);
  }

  return (
    map.hasSource(COUNTRY_GEOMETRY_SOURCE_ID) &&
    map.hasImage(TRANSPARENT_PATTERN_ID) &&
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

function patternImageId(fingerprint: string): string {
  return `${TONE_PATTERN_PREFIX}${fingerprint}`;
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
      map.addLayer(layer);
    },
    hasImage(id) {
      return map.hasImage(id);
    },
    addImage(id, image) {
      map.addImage(id, image);
    },
    removeImage(id) {
      map.removeImage(id);
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
