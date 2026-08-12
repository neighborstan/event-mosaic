import type { AddLayerObject, MapOptions } from "maplibre-gl";

import type { CountryGeometry } from "../features/country-map/api/countryGeometry";
import type {
  MapLibreCountryEvent,
  MapLibreMapFactory,
  MapLibreOwnedEvent,
  MapLibreOwnedMap,
  MapLibreOwnedSubscription,
} from "../features/country-map/lib/useMapLibreMap";

type Listener = () => void;
type CountryListener = (regionId: string | null) => void;

const TONE_PATTERN_ID_PREFIX = "event-mosaic-country-pattern:";

interface SubscriptionRecord {
  event: MapLibreOwnedEvent | MapLibreCountryEvent | "background-select";
  unsubscribeCalls: number;
}

export class ControlledMapLibreMap implements MapLibreOwnedMap {
  private readonly listeners: Record<MapLibreOwnedEvent, Set<Listener>> = {
    load: new Set<Listener>(),
    "style.load": new Set<Listener>(),
    error: new Set<Listener>(),
  };

  private readonly countryListeners: Record<
    MapLibreCountryEvent,
    Set<CountryListener>
  > = {
    hover: new Set<CountryListener>(),
    leave: new Set<CountryListener>(),
    select: new Set<CountryListener>(),
  };

  private readonly backgroundSelectListeners = new Set<Listener>();
  private readonly subscriptions: SubscriptionRecord[] = [];

  readonly sourceAdditions: Readonly<{
    id: string;
    geometry: CountryGeometry;
    promoteId: "regionId";
  }>[] = [];

  readonly layerAdditions: AddLayerObject[] = [];
  readonly imageAdditions: Readonly<{
    id: string;
    width: number;
    height: number;
    data: Uint8Array | Uint8ClampedArray;
  }>[] = [];
  readonly imageRemovals: string[] = [];
  readonly featureStateUpdates: Readonly<{
    regionId: string;
    state: Readonly<Record<string, string | boolean>>;
  }>[] = [];
  readonly operationLog: string[] = [];
  readonly countrySubscriptionLayers: Readonly<{
    event: MapLibreCountryEvent;
    layerId: string;
  }>[] = [];

  private readonly sourceIds = new Set<string>();
  private readonly layerIds = new Set<string>();
  private readonly images = new Map<
    string,
    Readonly<{
      width: number;
      height: number;
      data: Uint8Array | Uint8ClampedArray;
    }>
  >();
  private readonly featureStates = new Map<
    string,
    Record<string, string | boolean>
  >();
  private maximumToneImageCountValue = 0;

  removeCalls = 0;

  subscribe(
    event: MapLibreOwnedEvent,
    listener: Listener,
  ): MapLibreOwnedSubscription {
    return this.addSubscription(event, this.listeners[event], listener);
  }

  subscribeCountry(
    event: MapLibreCountryEvent,
    layerId: string,
    listener: CountryListener,
  ): MapLibreOwnedSubscription {
    this.countrySubscriptionLayers.push({ event, layerId });
    return this.addSubscription(event, this.countryListeners[event], listener);
  }

  subscribeBackgroundSelect(listener: Listener): MapLibreOwnedSubscription {
    return this.addSubscription(
      "background-select",
      this.backgroundSelectListeners,
      listener,
    );
  }

  hasSource(id: string): boolean {
    return this.sourceIds.has(id);
  }

  addCountryGeometrySource(
    id: string,
    geometry: CountryGeometry,
    promoteId: "regionId",
  ): void {
    if (this.sourceIds.has(id)) {
      throw new Error(`Тестовая карта уже содержит source ${id}`);
    }

    this.sourceIds.add(id);
    this.sourceAdditions.push({ id, geometry, promoteId });
    this.operationLog.push(`source:${id}`);
  }

  hasLayer(id: string): boolean {
    return this.layerIds.has(id);
  }

  addLayer(layer: AddLayerObject): void {
    if (this.layerIds.has(layer.id)) {
      throw new Error(`Тестовая карта уже содержит layer ${layer.id}`);
    }

    this.layerIds.add(layer.id);
    this.layerAdditions.push(layer);
    this.operationLog.push(`layer:${layer.id}`);
  }

  hasImage(id: string): boolean {
    return this.images.has(id);
  }

  addImage(
    id: string,
    image: Readonly<{
      width: number;
      height: number;
      data: Uint8Array | Uint8ClampedArray;
    }>,
  ): void {
    if (this.images.has(id)) {
      throw new Error(`Тестовая карта уже содержит image ${id}`);
    }

    this.images.set(id, image);
    this.maximumToneImageCountValue = Math.max(
      this.maximumToneImageCountValue,
      this.activeToneImageCount(),
    );
    this.imageAdditions.push({ id, ...image });
    this.operationLog.push(`image:${id}`);
  }

  removeImage(id: string): void {
    if (!this.images.delete(id)) {
      throw new Error(`Тестовая карта не содержит image ${id}`);
    }

    this.imageRemovals.push(id);
    this.operationLog.push(`remove-image:${id}`);
  }

  setCountryFeatureState(
    regionId: string,
    state: Readonly<Record<string, string | boolean>>,
  ): void {
    const currentState = this.featureStates.get(regionId) ?? {};
    this.featureStates.set(regionId, { ...currentState, ...state });
    this.featureStateUpdates.push({ regionId, state });
    this.operationLog.push(`feature-state:${regionId}`);
  }

  remove(): void {
    this.removeCalls += 1;
  }

  emit(event: MapLibreOwnedEvent): void {
    for (const listener of [...this.listeners[event]]) {
      listener();
    }
  }

  emitCountry(event: MapLibreCountryEvent, regionId: string | null): void {
    for (const listener of [...this.countryListeners[event]]) {
      listener(regionId);
    }
  }

  emitBackgroundSelect(): void {
    for (const listener of [...this.backgroundSelectListeners]) {
      listener();
    }
  }

  simulateStyleReload(): void {
    this.sourceIds.clear();
    this.layerIds.clear();
    this.images.clear();
    this.featureStates.clear();
    this.operationLog.push("style-reset");
    this.emit("style.load");
  }

  activeListenerCount(event: MapLibreOwnedEvent): number {
    return this.listeners[event].size;
  }

  activeCountryListenerCount(event: MapLibreCountryEvent): number {
    return this.countryListeners[event].size;
  }

  activeBackgroundSelectListenerCount(): number {
    return this.backgroundSelectListeners.size;
  }

  unsubscribeCallCount(
    event: MapLibreOwnedEvent | MapLibreCountryEvent | "background-select",
  ): number {
    return this.subscriptions
      .filter((subscription) => subscription.event === event)
      .reduce(
        (total, subscription) => total + subscription.unsubscribeCalls,
        0,
      );
  }

  activeImageIds(): readonly string[] {
    return [...this.images.keys()];
  }

  maximumActiveToneImageCount(): number {
    return this.maximumToneImageCountValue;
  }

  stateFor(regionId: string): Readonly<Record<string, string | boolean>> {
    return this.featureStates.get(regionId) ?? {};
  }

  private activeToneImageCount(): number {
    return [...this.images.keys()].filter((imageId) =>
      imageId.startsWith(TONE_PATTERN_ID_PREFIX),
    ).length;
  }

  private addSubscription<T>(
    event: SubscriptionRecord["event"],
    listeners: Set<T>,
    listener: T,
  ): MapLibreOwnedSubscription {
    const record: SubscriptionRecord = {
      event,
      unsubscribeCalls: 0,
    };
    let isActive = true;

    listeners.add(listener);
    this.subscriptions.push(record);

    return {
      unsubscribe: () => {
        record.unsubscribeCalls += 1;

        if (isActive) {
          isActive = false;
          listeners.delete(listener);
        }
      },
    };
  }
}

export type ControlledMapLibreFactory = Readonly<{
  factory: MapLibreMapFactory;
  maps: ControlledMapLibreMap[];
  options: MapOptions[];
}>;

export function createControlledMapLibreFactory(): ControlledMapLibreFactory {
  const maps: ControlledMapLibreMap[] = [];
  const options: MapOptions[] = [];
  const factory: MapLibreMapFactory = (mapOptions) => {
    const map = new ControlledMapLibreMap();

    options.push(mapOptions);
    maps.push(map);
    return map;
  };

  return {
    factory,
    maps,
    options,
  };
}

export function requireControlledMap(
  fixture: ControlledMapLibreFactory,
  index = 0,
): ControlledMapLibreMap {
  const map = fixture.maps[index];

  if (map === undefined) {
    throw new Error(`Не найдена тестовая карта с индексом ${index}`);
  }

  return map;
}
