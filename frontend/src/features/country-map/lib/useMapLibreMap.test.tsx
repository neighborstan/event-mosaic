import { act, render, screen } from "@testing-library/react";
import { StrictMode, useRef } from "react";
import { describe, expect, test } from "vitest";
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
  useMapLibreMap,
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
}>;

function MapHarness({
  mapFactory,
  geometry = null,
}: MapHarnessProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const state = useMapLibreMap({
    containerRef,
    style: LOCAL_STYLE,
    attributionControl: {
      compact: false,
    },
    geometry,
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
  test("Создает одну карту мира и освобождает все подписки при удалении", () => {
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
    expect(map.activeListenerCount("error")).toBe(1);

    view.rerender(<MapHarness mapFactory={fixture.factory} />);

    expect(fixture.maps).toHaveLength(1);

    view.unmount();

    expect(map.activeListenerCount("load")).toBe(0);
    expect(map.activeListenerCount("error")).toBe(0);
    expect(map.unsubscribeCallCount("load")).toBe(1);
    expect(map.unsubscribeCallCount("error")).toBe(1);
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
    expect(firstMap.activeListenerCount("error")).toBe(0);
    expect(secondMap.removeCalls).toBe(0);
    expect(secondMap.activeListenerCount("load")).toBe(1);
    expect(secondMap.activeListenerCount("error")).toBe(1);

    view.unmount();

    expect(secondMap.removeCalls).toBe(1);
    expect(secondMap.activeListenerCount("load")).toBe(0);
    expect(secondMap.activeListenerCount("error")).toBe(0);
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

  test("Ошибка отдельного ресурса после загрузки стиля не объявляет отказ фоновой карты", () => {
    const fixture = createControlledMapLibreFactory();
    render(<MapHarness mapFactory={fixture.factory} />);
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
      map.emit("error");
    });

    expect(screen.getByTestId("map-state")).toHaveTextContent("loading");
    expect(fixture.maps).toHaveLength(1);
  });

  test("Успешная загрузка стиля после ранней ошибки возвращает состояние загрузки", () => {
    const fixture = createControlledMapLibreFactory();
    render(<MapHarness mapFactory={fixture.factory} />);
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("error");
    });
    expect(screen.getByTestId("map-state")).toHaveTextContent("provider-error");

    act(() => {
      map.emit("load");
    });
    expect(screen.getByTestId("map-state")).toHaveTextContent("loading");
    expect(fixture.maps).toHaveLength(1);
  });

  test("Добавляет проверенную геометрию после готовности стиля в порядке источника, заливки и контура", () => {
    const fixture = createControlledMapLibreFactory();
    const geometry = createValidatedCountryGeometry();
    render(<MapHarness mapFactory={fixture.factory} geometry={geometry} />);
    const map = requireControlledMap(fixture);

    expect(map.sourceAdditions).toHaveLength(0);
    expect(map.layerAdditions).toHaveLength(0);

    act(() => {
      map.emit("load");
    });

    expect(screen.getByTestId("map-state")).toHaveTextContent("ready");
    expect(map.sourceAdditions).toEqual([
      {
        id: COUNTRY_GEOMETRY_SOURCE_ID,
        geometry,
      },
    ]);
    expect(map.layerAdditions).toEqual([
      {
        id: COUNTRY_FILL_LAYER_ID,
        type: "fill",
        source: COUNTRY_GEOMETRY_SOURCE_ID,
        paint: {
          "fill-color": "#4f7175",
          "fill-opacity": 0.28,
        },
      },
      {
        id: COUNTRY_LINE_LAYER_ID,
        type: "line",
        source: COUNTRY_GEOMETRY_SOURCE_ID,
        paint: {
          "line-color": "#2f494d",
          "line-width": 0.8,
        },
      },
    ]);
    expect(map.operationLog).toEqual([
      `source:${COUNTRY_GEOMETRY_SOURCE_ID}`,
      `layer:${COUNTRY_FILL_LAYER_ID}`,
      `layer:${COUNTRY_LINE_LAYER_ID}`,
    ]);
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
    expect(map.layerAdditions).toHaveLength(2);
    expect(screen.getByTestId("map-state")).toHaveTextContent("ready");
  });

  test("Повторные события готовности стиля не добавляют источник и слои второй раз", () => {
    const fixture = createControlledMapLibreFactory();
    render(
      <MapHarness
        mapFactory={fixture.factory}
        geometry={createValidatedCountryGeometry()}
      />,
    );
    const map = requireControlledMap(fixture);

    act(() => {
      map.emit("load");
      map.emit("load");
    });

    expect(map.sourceAdditions).toHaveLength(1);
    expect(map.layerAdditions).toHaveLength(2);
    expect(screen.getByTestId("map-state")).toHaveTextContent("ready");
  });
});
