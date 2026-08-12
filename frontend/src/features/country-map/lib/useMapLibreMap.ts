import { Map as MapLibreMap } from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import type { MapOptions } from "maplibre-gl";
import type { RefObject } from "react";

export type MapLibreStyle = NonNullable<MapOptions["style"]>;

export type MapLibreTechnicalState =
  Readonly<{ status: "loading" }> | Readonly<{ status: "provider-error" }>;

export type MapLibreOwnedEvent = "load" | "error";

export interface MapLibreOwnedSubscription {
  unsubscribe(): void;
}

export interface MapLibreOwnedMap {
  subscribe(
    event: MapLibreOwnedEvent,
    listener: () => void,
  ): MapLibreOwnedSubscription;
  remove(): void;
}

export type MapLibreMapFactory = (options: MapOptions) => MapLibreOwnedMap;

type UseMapLibreMapOptions = Readonly<{
  containerRef: RefObject<HTMLDivElement | null>;
  style: MapLibreStyle;
  attributionControl: Readonly<{
    compact: false;
  }>;
  mapFactory?: MapLibreMapFactory | undefined;
}>;

const LOADING_STATE = {
  status: "loading",
} as const satisfies MapLibreTechnicalState;

const PROVIDER_ERROR_STATE = {
  status: "provider-error",
} as const satisfies MapLibreTechnicalState;

/**
 * Создает одну карту, владеет ее начальными событиями и полностью освобождает
 * MapLibre при удалении React-компонента.
 */
export function useMapLibreMap({
  containerRef,
  style,
  attributionControl,
  mapFactory = createMapLibreMap,
}: UseMapLibreMapOptions): MapLibreTechnicalState {
  const mapRef = useRef<MapLibreOwnedMap | null>(null);
  const attributionCompact = attributionControl.compact;
  const [technicalState, setTechnicalState] =
    useState<MapLibreTechnicalState>(LOADING_STATE);

  useEffect(() => {
    const container = containerRef.current;

    if (container === null) {
      return undefined;
    }

    let isOwned = true;
    let isStyleLoaded = false;
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
        isStyleLoaded = true;
        setTechnicalState(LOADING_STATE);
      }
    });
    const errorSubscription = map.subscribe("error", () => {
      if (isOwned && !isStyleLoaded) {
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
      }
    };
  }, [attributionCompact, containerRef, mapFactory, style]);

  return technicalState;
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
    remove() {
      map.remove();
    },
  };
}
