import { act, render, screen } from "@testing-library/react";
import { StrictMode } from "react";
import { describe, expect, test } from "vitest";

import { createValidatedCountryGeometry } from "../../../test/countryGeometryFixtures";
import {
  createControlledMapLibreFactory,
  requireControlledMap,
} from "../../../test/controlledMapLibre";
import type { CountryGeometry } from "../api/countryGeometry";
import type { CountryGeometryLoader } from "../api/useCountryGeometry";
import { CountryMapScreen } from "./CountryMapScreen";

interface ControlledGeometryRequest {
  signal: AbortSignal;
  resolve(geometry: CountryGeometry): void;
  reject(error: unknown): void;
}

interface ControlledGeometryLoader {
  loader: CountryGeometryLoader;
  requests: ControlledGeometryRequest[];
}

describe("Экран нейтральной карты стран", () => {
  test("Показывает загрузку до готовности стиля и геометрии, затем объявляет готовность слоя стран", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    expect(screen.getByRole("main")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", {
        level: 1,
        name: "Карта событий по странам",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("region", { name: "Область карты стран" }),
    ).toHaveAttribute("aria-busy", "true");
    expect(
      screen.getByRole("region", { name: "Фоновая карта мира" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Карта загружается. Ожидаем фоновый слой и проверенные границы стран.",
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    expect(screen.getByRole("status")).toHaveTextContent("Карта загружается");

    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      await Promise.resolve();
    });

    expect(screen.getByRole("status")).toHaveTextContent(
      "Карта готова. Показаны границы 258 стран и территорий.",
    );
    expect(
      screen.getByRole("region", { name: "Область карты стран" }),
    ).toHaveAttribute("aria-busy", "false");
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(1);
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(2);
  });

  test("Неизвестный источник фоновой карты показывает безопасную ошибку до карты и HTTP", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        providerValue="private-provider-value"
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Источник фоновой карты настроен неверно",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent(
      "private-provider-value",
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(mapFixture.maps).toHaveLength(0);
    expect(mapFixture.options).toHaveLength(0);
    expect(geometryFixture.requests).toHaveLength(0);
  });

  test("Явно выбранный Stadia использует тот же владеющий адаптер", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        providerValue="stadia"
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    expect(mapFixture.maps).toHaveLength(1);
    expect(mapFixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.stadiamaps.com/styles/alidade_smooth.json",
        attributionControl: {
          compact: false,
        },
      }),
    ]);
    expect(screen.getByRole("status")).toHaveTextContent("Карта загружается");
    expect(geometryFixture.requests).toHaveLength(1);
  });

  test("Ошибка OpenFreeMap видна текстом и не запускает автоматическое переключение", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("error");
    });

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Не удалось загрузить выбранную фоновую карту",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Автоматическое переключение источника не выполняется",
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(mapFixture.maps).toHaveLength(1);
    expect(mapFixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.openfreemap.org/styles/liberty",
      }),
    ]);
  });

  test("Ошибка геометрии отличается от ошибки фоновой карты и не выглядит как ноль событий", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await act(async () => {
      requireGeometryRequest(geometryFixture).reject(
        new Error("Поврежденный ответ"),
      );
      await Promise.resolve();
    });

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Не удалось загрузить или проверить геометрию стран",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "не отображаются как пустая карта",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent(
      "Поврежденный ответ",
    );
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(0);
  });

  test("Удаление экрана отменяет загрузку геометрии и освобождает MapLibre без поздней публикации", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const view = render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );
    const request = requireGeometryRequest(geometryFixture);
    const map = requireControlledMap(mapFixture);

    view.unmount();

    expect(request.signal.aborted).toBe(true);
    expect(map.removeCalls).toBe(1);
    expect(map.activeListenerCount("load")).toBe(0);
    expect(map.activeListenerCount("error")).toBe(0);
    expect(map.sourceAdditions).toHaveLength(0);
  });

  test("Strict Mode отменяет первый запрос и оставляет повторно созданный экран готовым", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <StrictMode>
        <CountryMapScreen
          geometryLoader={geometryFixture.loader}
          mapFactory={mapFixture.factory}
        />
      </StrictMode>,
    );
    const firstRequest = requireGeometryRequest(geometryFixture, 0);
    const secondRequest = requireGeometryRequest(geometryFixture, 1);
    const firstMap = requireControlledMap(mapFixture, 0);
    const secondMap = requireControlledMap(mapFixture, 1);

    expect(firstRequest.signal.aborted).toBe(true);
    expect(secondRequest.signal.aborted).toBe(false);
    expect(firstMap.removeCalls).toBe(1);

    act(() => {
      secondMap.emit("load");
    });
    await act(async () => {
      secondRequest.resolve(createValidatedCountryGeometry());
      await Promise.resolve();
    });

    expect(screen.getByRole("status")).toHaveTextContent("Карта готова");
    expect(secondMap.sourceAdditions).toHaveLength(1);
    expect(secondMap.layerAdditions).toHaveLength(2);
  });

  test("Готовая основа не добавляет легенду, выбор страны или управляющие элементы", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      await Promise.resolve();
    });

    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
    expect(screen.queryByText(/легенд/i)).not.toBeInTheDocument();
    expect(requireControlledMap(mapFixture).activeListenerCount("load")).toBe(
      1,
    );
    expect(requireControlledMap(mapFixture).activeListenerCount("error")).toBe(
      1,
    );
  });
});

function createControlledGeometryLoader(): ControlledGeometryLoader {
  const requests: ControlledGeometryRequest[] = [];
  const loader: CountryGeometryLoader = (signal) =>
    new Promise((resolve, reject) => {
      requests.push({ signal, resolve, reject });
      signal.addEventListener(
        "abort",
        () => {
          reject(new DOMException("Aborted", "AbortError"));
        },
        { once: true },
      );
    });

  return { loader, requests };
}

function requireGeometryRequest(
  fixture: ControlledGeometryLoader,
  index = 0,
): ControlledGeometryRequest {
  const request = fixture.requests[index];

  if (request === undefined) {
    throw new Error(`Не найден тестовый запрос geometry с индексом ${index}`);
  }

  return request;
}
