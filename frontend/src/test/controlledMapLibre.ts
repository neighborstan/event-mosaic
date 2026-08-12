import type { MapOptions } from "maplibre-gl";

import type {
  MapLibreMapFactory,
  MapLibreOwnedEvent,
  MapLibreOwnedMap,
  MapLibreOwnedSubscription,
} from "../features/country-map/lib/useMapLibreMap";

type Listener = () => void;

interface SubscriptionRecord {
  event: MapLibreOwnedEvent;
  unsubscribeCalls: number;
}

export class ControlledMapLibreMap implements MapLibreOwnedMap {
  private readonly listeners: Record<MapLibreOwnedEvent, Set<Listener>> = {
    load: new Set<Listener>(),
    error: new Set<Listener>(),
  };

  private readonly subscriptions: SubscriptionRecord[] = [];

  removeCalls = 0;

  subscribe(
    event: MapLibreOwnedEvent,
    listener: Listener,
  ): MapLibreOwnedSubscription {
    const listeners = this.listeners[event];
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

  remove(): void {
    this.removeCalls += 1;
  }

  emit(event: MapLibreOwnedEvent): void {
    for (const listener of [...this.listeners[event]]) {
      listener();
    }
  }

  activeListenerCount(event: MapLibreOwnedEvent): number {
    return this.listeners[event].size;
  }

  unsubscribeCallCount(event: MapLibreOwnedEvent): number {
    return this.subscriptions
      .filter((subscription) => subscription.event === event)
      .reduce(
        (total, subscription) => total + subscription.unsubscribeCalls,
        0,
      );
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
