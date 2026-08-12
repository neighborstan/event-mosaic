import { act, render, screen } from "@testing-library/react";
import { StrictMode, useRef } from "react";
import { describe, expect, test, vi } from "vitest";
import type { JSX } from "react";

import { createValidatedCountryGeometry } from "../../../test/countryGeometryFixtures";
import {
  createControlledMapLibreFactory,
  requireControlledMap,
} from "../../../test/controlledMapLibre";
import {
  COUNTRY_FILL_LAYER_ID,
  COUNTRY_GEOMETRY_SOURCE_ID,
  COUNTRY_LINE_LAYER_ID,
  COUNTRY_STATE_LAYER_ID,
  COUNTRY_TONE_LAYER_ID,
  useMapLibreMap,
  type CountryMapRegionVisual,
  type MapLibreMapFactory,
  type MapLibreStyle,
} from "./useMapLibreMap";
import type { CountryGeometry } from "../api/countryGeometry";

const LOCAL_STYLE = {
  version: 8,
  sources: {},
  layers: [],
} satisfies MapLibreStyle;

type MapHarnessProps = Readonly<{
  mapFactory: MapLibreMapFactory;
  geometry?: CountryGeometry | null;
  visualRegions?: readonly CountryMapRegionVisual[] | null;
  hoveredRegionId?: string | null;
  selectedRegionId?: string | null;
  onHoverRegion?: ((regionId: string | null) => void) | undefined;
  onSelectRegion?: ((regionId: string | null) => void) | undefined;
}>;

function MapHarness({
  mapFactory,
  geometry = null,
  visualRegions = null,
  hoveredRegionId = null,
  selectedRegionId = null,
  onHoverRegion = () => undefined,
  onSelectRegion = () => undefined,
}: MapHarnessProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const state = useMapLibreMap({
    containerRef,
    style: LOCAL_STYLE,
    attributionControl: {
      compact: false,
    },
    geometry,
    visualRegions,
    hoveredRegionId,
    selectedRegionId,
    onHoverRegion,
    onSelectRegion,
    mapFactory,
  });

  return (
    <section>
      <div ref={containerRef} role="region" aria-label="Тестовая карта" />
      <output data-testid="map-state">{state.status}</output>
    </section>
  );
}

describe("Владение жизненным циклом MapLibre", () => {
  test("Создает одну карту и освобождает все технические и смысловые подписки", () => {
    const fixture = createControlledMapLibreFactory();
    const view = render(<MapHarness mapFactory={fixture.factory} />);
    const map = requireControlledMap(fixture);

    expect(fixture.maps).toHaveLength(1);
    expect(fixture.options).toEqual([
      expect.objectContaining({
        container: screen.getByRole("region", { name: "Тестовая карта" }),
        style: LOCAL_STYLE,
        center: [0, 20],
        zoom: 1,
        attributionControl: {
          compact: false,
        },
      }),
    ]);
    expect(map.activeListenerCount("load")).toBe(1);
    expect(map.activeListenerCount("style.load")).toBe(1);
    expect(map.activeListenerCount("error")).toBe(1);
    expect(map.activeCountryListenerCount("hover")).toBe(1);
    expect(map.activeCountryListenerCount("leave")).toBe(1);
    expect(map.activeCountryListenerCount("select")).toBe(1);
    expect(map.activeBackgroundSelectListenerCount()).toBe(1);
    expect(map.countrySubscriptionLayers).toEqual([
      { event: "hover", layerId: COUNTRY_FILL_LAYER_ID },
      { event: "leave", layerId: COUNTRY_FILL_LAYER_ID },
      { event: "select", layerId: COUNTRY_FILL_LAYER_ID },
    ]);

    view.rerender(<MapHarness mapFactory={fixture.factory} />);
    expect(fixture.maps).toHaveLength(1);

    view.unmount();

    expect(map.activeListenerCount("load")).toBe(0);
    expect(map.activeListenerCount("style.load")).toBe(0);
    expect(map.activeListenerCount("error")).toBe(0);
    expect(map.activeCountryListenerCount("hover")).toBe(0);
    expect(map.activeCountryListenerCount("leave")).toBe(0);
    expect(map.activeCountryListenerCount("select")).toBe(0);
    expect(map.activeBackgroundSelectListenerCount()).toBe(0);
    expect(map.unsubscribeCallCount("background-select")).toBe(1);
    expect(map.removeCalls).toBe(1);
  });

  test("Повторное создание в Strict Mode оставляет только второй активный экземпляр", () => {
    const fixture = createControlledMapLibreFactory();
    const view = render(
      <StrictMode>
        <MapHarness mapFactory={fixture.factory} />
      </StrictMode>,
    );
    const firstMap = requireControlledMap(fixture, 0);
    const secondMap = requireControlledMap(fixture, 1);

    expect(fixture.maps).toHaveLength(2);
    expect(firstMap.removeCalls).toBe(1);
    expect(firstMap.activeListenerCount("load")).toBe(0);
    expect(firstMap.activeCountryListenerCount("select")).toBe(0);
    expect(secondMap.removeCalls).toBe(0);
    expect(secondMap.activeListenerCount("load")).toBe(1);
    expect(secondMap.activeCountryListenerCount("select")).toBe(1);

    view.unmount();
    expect(secondMap.removeCalls).toBe(1);
    expect(secondMap.activeListenerCount("load")).toBe(0);
  });

  test("Ошибка до загрузки стиля публикует только безопасное техническое состояние", () => {
    const fixture = createControlledMapLibreFactory();
    render(<MapHarness mapFactory={fixture.factory} />);

    act(() => {
      requireControlledMap(fixture).emit("error");
    });

    expect(screen.getByTestId("map-state")).toHaveTextContent("provider-error");
    expect(fixture.maps).toHaveLength(1);
  });

  test("Добавляет источник со стабильным regionId и четыре owned layers", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    render(<MapHarness mapFactory={fixture.factory} geometry={geometry} />);
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
    });

    expect(screen.getByTestId("map-state")).toHaveTextContent("ready");
    expect(map.sourceAdditions).toEqual([
      {
        id: COUNTRY_GEOMETRY_SOURCE_ID,
        geometry,
        promoteId: "regionId",
      },
    ]);
    expect(map.layerAdditions.map((layer) => layer.id)).toEqual([
      COUNTRY_FILL_LAYER_ID,
      COUNTRY_TONE_LAYER_ID,
      COUNTRY_LINE_LAYER_ID,
      COUNTRY_STATE_LAYER_ID,
    ]);
    expect(map.activeImageIds()).toHaveLength(1);
  });

  test("Синхронизирует геометрию после стиля без пересоздания карты", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    const view = render(<MapHarness mapFactory={fixture.factory} />);
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
    });
    expect(screen.getByTestId("map-state")).toHaveTextContent("loading");

    view.rerender(
      <MapHarness mapFactory={fixture.factory} geometry={geometry} />,
    );

    expect(fixture.maps).toHaveLength(1);
    expect(map.sourceAdditions).toHaveLength(1);
    expect(map.layerAdditions).toHaveLength(4);
    expect(screen.getByTestId("map-state")).toHaveTextContent("ready");
  });

  test("Неизменный refresh переиспользует image, а измененный безопасно переключает pattern", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    const regionId = geometry.features[0]?.properties.regionId ?? "country:r0";
    const firstVisual = [toneVisual(regionId, "first", 31)];
    const secondVisual = [toneVisual(regionId, "second", 47)];
    const view = render(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={firstVisual}
      />,
    );
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
    });
    const additionsAfterFirstSnapshot = map.imageAdditions.length;

    view.rerender(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={firstVisual}
      />,
    );
    expect(map.imageAdditions).toHaveLength(additionsAfterFirstSnapshot);

    const operationCountBeforeRefresh = map.operationLog.length;
    view.rerender(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={secondVisual}
      />,
    );

    expect(map.imageAdditions).toHaveLength(additionsAfterFirstSnapshot + 1);
    expect(map.imageRemovals).toHaveLength(1);
    const latestPatternState = map.stateFor(regionId)["patternImageId"];
    expect(latestPatternState).toEqual(expect.stringContaining("second"));
    const refreshOperations = map.operationLog.slice(
      operationCountBeforeRefresh,
    );
    const transparentStateOperation = refreshOperations.findIndex(
      (operation) => operation === `feature-state:${regionId}`,
    );
    const removalOperation = refreshOperations.findIndex((operation) =>
      operation.startsWith("remove-image:"),
    );
    const additionOperation = refreshOperations.findIndex((operation) =>
      operation.includes("pattern:second"),
    );
    const finalStateOperation = refreshOperations.lastIndexOf(
      `feature-state:${regionId}`,
    );
    expect(transparentStateOperation).toBeLessThan(removalOperation);
    expect(removalOperation).toBeLessThan(additionOperation);
    expect(additionOperation).toBeLessThan(finalStateOperation);
    expect(map.maximumActiveToneImageCount()).toBe(1);
    expect(fixture.maps).toHaveLength(1);
    expect(map.sourceAdditions).toHaveLength(1);
  });

  test("Настоящий style reload восстанавливает source, images, layers и state без новых listeners", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    const regionId = geometry.features[0]?.properties.regionId ?? "country:r0";
    render(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={[toneVisual(regionId, "stable", 53)]}
      />,
    );
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
    });
    const initialImageCount = map.imageAdditions.length;

    act(() => {
      map.simulateStyleReload();
    });

    expect(map.sourceAdditions).toHaveLength(2);
    expect(map.layerAdditions).toHaveLength(8);
    expect(map.imageAdditions).toHaveLength(initialImageCount * 2);
    expect(map.stateFor(regionId)).toEqual(
      expect.objectContaining({ visualStatus: "tone-mixture" }),
    );
    expect(map.activeListenerCount("style.load")).toBe(1);
    expect(map.activeCountryListenerCount("hover")).toBe(1);
    expect(fixture.maps).toHaveLength(1);
  });

  test("Ограничивает generated images точным roster из 258 стран", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    const visualRegions = geometry.features.map((feature, index) =>
      toneVisual(
        feature.properties.regionId,
        `fingerprint-${index.toString()}`,
        index % 255,
      ),
    );
    const view = render(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={visualRegions}
      />,
    );
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
    });

    expect(
      map
        .activeImageIds()
        .filter((imageId) => imageId.includes("pattern:fingerprint-")),
    ).toHaveLength(258);
    expect(map.featureStateUpdates).toHaveLength(258);
    expect(map.maximumActiveToneImageCount()).toBe(258);

    const refreshedVisualRegions = geometry.features.map((feature, index) =>
      toneVisual(
        feature.properties.regionId,
        `replacement-${index.toString()}`,
        (index + 1) % 255,
      ),
    );
    view.rerender(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={refreshedVisualRegions}
      />,
    );

    expect(
      map
        .activeImageIds()
        .filter((imageId) => imageId.includes("pattern:replacement-")),
    ).toHaveLength(258);
    expect(map.imageRemovals).toHaveLength(258);
    expect(map.maximumActiveToneImageCount()).toBe(258);
    expect(fixture.maps).toHaveLength(1);
    expect(map.sourceAdditions).toHaveLength(1);
  });

  test("Смысловые события сообщают только regionId и отдельный выбор фона", () => {
    const fixture = createControlledMapLibreFactory();
    const onHoverRegion = vi.fn<(regionId: string | null) => void>();
    const onSelectRegion = vi.fn<(regionId: string | null) => void>();
    render(
      <MapHarness
        mapFactory={fixture.factory}
        onHoverRegion={onHoverRegion}
        onSelectRegion={onSelectRegion}
      />,
    );
    const map = requireControlledMap(fixture);

    act(() => {
      map.emitCountry("hover", "country:r1");
      map.emitCountry("leave", null);
      map.emitCountry("select", "country:r2");
      map.emitBackgroundSelect();
    });

    expect(onHoverRegion.mock.calls).toEqual([["country:r1"], [null]]);
    expect(onSelectRegion.mock.calls).toEqual([["country:r2"], [null]]);
  });

  test("Hover и selection меняют только feature state и не пересоздают resources", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    const firstRegionId =
      geometry.features[0]?.properties.regionId ?? "country:r0";
    const secondRegionId =
      geometry.features[1]?.properties.regionId ?? "country:r1";
    const visualRegions = [
      toneVisual(firstRegionId, "first", 61),
      noEventsVisual(secondRegionId),
    ];
    const view = render(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={visualRegions}
      />,
    );
    const map = requireControlledMap(fixture);
    act(() => {
      map.emit("load");
    });
    const sourceCount = map.sourceAdditions.length;
    const layerCount = map.layerAdditions.length;
    const imageCount = map.imageAdditions.length;

    view.rerender(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={geometry}
        visualRegions={visualRegions}
        hoveredRegionId={secondRegionId}
        selectedRegionId={firstRegionId}
      />,
    );

    expect(map.stateFor(firstRegionId)["selected"]).toBe(true);
    expect(map.stateFor(secondRegionId)["hovered"]).toBe(true);
    expect(map.sourceAdditions).toHaveLength(sourceCount);
    expect(map.layerAdditions).toHaveLength(layerCount);
    expect(map.imageAdditions).toHaveLength(imageCount);
    expect(fixture.maps).toHaveLength(1);
  });
});

function toneVisual(
  regionId: string,
  fingerprint: string,
  byte: number,
): CountryMapRegionVisual {
  return {
    regionId,
    status: "tone-mixture",
    pattern: {
      fingerprint,
      rgba: new Uint8ClampedArray(64 * 64 * 4).fill(byte),
      width: 64,
      height: 64,
    },
  };
}

function noEventsVisual(regionId: string): CountryMapRegionVisual {
  return {
    regionId,
    status: "no-events",
  };
}
