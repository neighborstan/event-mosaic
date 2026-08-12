import { act, render, screen } from "@testing-library/react";
import { StrictMode, useRef } from "react";
import { describe, expect, test } from "vitest";
import type { JSX } from "react";

import {
  createControlledMapLibreFactory,
  requireControlledMap,
} from "../../../test/controlledMapLibre";
import {
  useMapLibreMap,
  type MapLibreMapFactory,
  type MapLibreStyle,
} from "./useMapLibreMap";

const LOCAL_STYLE = {
  version: 8,
  sources: {},
  layers: [],
} satisfies MapLibreStyle;

type MapHarnessProps = Readonly<{
  mapFactory: MapLibreMapFactory;
}>;

function MapHarness({ mapFactory }: MapHarnessProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const state = useMapLibreMap({
    containerRef,
    style: LOCAL_STYLE,
    attributionControl: {
      compact: false,
    },
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

  test("Повторный mount в Strict Mode оставляет только второй активный экземпляр", () => {
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

  test("Ошибка отдельного ресурса после загрузки стиля не объявляет отказ provider", () => {
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
});
