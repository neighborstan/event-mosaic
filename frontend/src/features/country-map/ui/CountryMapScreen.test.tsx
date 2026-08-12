import { act, render, screen } from "@testing-library/react";
import { StrictMode } from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import {
  createCountryGeometryDocument,
  createValidatedCountryGeometry,
} from "../../../test/countryGeometryFixtures";
import {
  createCountrySnapshotDocument,
  createValidatedCountrySnapshot,
} from "../../../test/countrySnapshotFixtures";
import {
  createControlledMapLibreFactory,
  requireControlledMap,
} from "../../../test/controlledMapLibre";
import {
  validateCountryGeometry,
  type CountryGeometry,
} from "../api/countryGeometry";
import {
  validateCountrySnapshot,
  type CountryCoverageStatus,
  type CountrySnapshot,
} from "../api/countrySnapshot";
import type { CountryGeometryLoader } from "../api/useCountryGeometry";
import type { CountrySnapshotLoader } from "../api/useCountrySnapshot";
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

interface ControlledSnapshotRequest {
  signal: AbortSignal;
  resolve(snapshot: CountrySnapshot): void;
  reject(error: unknown): void;
}

interface ControlledSnapshotLoader {
  loader: CountrySnapshotLoader;
  requests: ControlledSnapshotRequest[];
}

afterEach(() => {
  vi.useRealTimers();
});

describe("Экран нейтральной карты с проверенным snapshot", () => {
  test("Запускает geometry и snapshot параллельно, но объявляет данные готовыми только после exact join", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        snapshotClock={() => new Date("2026-08-11T12:16:30Z")}
        mapFactory={mapFixture.factory}
      />,
    );

    await flushSnapshotStart();
    expect(geometryFixture.requests).toHaveLength(1);
    expect(snapshotFixture.requests).toHaveLength(1);
    expect(
      screen.getByRole("status", { name: "Состояние карты" }),
    ).toHaveTextContent("Карта загружается");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Данные событий загружаются");

    await act(async () => {
      requireSnapshotRequest(snapshotFixture).resolve(
        createValidatedCountrySnapshot(),
      );
      await Promise.resolve();
    });
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Данные событий загружаются");

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      await Promise.resolve();
    });

    expect(
      screen.getByRole("status", { name: "Состояние карты" }),
    ).toHaveTextContent("Карта готова. Показаны границы 258 стран");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent(
      "UTC-период [2026-08-10T12:15:00Z, 2026-08-11T12:15:00Z)",
    );
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Время принятия: 2026-08-11T12:16:30.000Z");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Сопоставлено событий: 4. Полнота: COMPLETE");
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(1);
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(2);
  });

  test("Неизвестный provider показывает безопасную ошибку до MapLibre и обоих HTTP owners", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        providerValue="private-provider-value"
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Источник фоновой карты настроен неверно",
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent(
      "private-provider-value",
    );
    expect(mapFixture.maps).toHaveLength(0);
    expect(geometryFixture.requests).toHaveLength(0);
    expect(snapshotFixture.requests).toHaveLength(0);
  });

  test("Явно выбранный Stadia использует тех же владельцев geometry и snapshot", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        providerValue="stadia"
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    await flushSnapshotStart();
    expect(mapFixture.maps).toHaveLength(1);
    expect(mapFixture.options).toEqual([
      expect.objectContaining({
        style: "https://tiles.stadiamaps.com/styles/alidade_smooth.json",
        attributionControl: { compact: false },
      }),
    ]);
    expect(geometryFixture.requests).toHaveLength(1);
    expect(snapshotFixture.requests).toHaveLength(1);
  });

  test("Ошибка provider остается отдельной от продолжающейся загрузки snapshot", () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("error");
    });

    expect(
      screen.getByRole("alert", { name: "Ошибка фоновой карты" }),
    ).toHaveTextContent("Автоматическое переключение источника не выполняется");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Данные событий загружаются");
    expect(mapFixture.maps).toHaveLength(1);
  });

  test("Ошибка geometry не превращает уже проверяемый snapshot в нулевые данные", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    await flushSnapshotStart();
    await act(async () => {
      requireSnapshotRequest(snapshotFixture).resolve(
        createValidatedCountrySnapshot(),
      );
      requireGeometryRequest(geometryFixture).reject(
        new Error("Поврежденный ответ geometry"),
      );
      await Promise.resolve();
    });

    expect(
      screen.getByRole("alert", { name: "Ошибка геометрии стран" }),
    ).toHaveTextContent("не отображаются как пустая карта");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Данные событий загружаются");
    expect(screen.getByRole("main")).not.toHaveTextContent(
      "Поврежденный ответ geometry",
    );
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(0);
  });

  test("Initial snapshot failure оставляет готовую geometry нейтральной и сообщает отдельную ошибку", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await flushSnapshotStart();
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      requireSnapshotRequest(snapshotFixture).reject(
        new Error("raw snapshot reason"),
      );
      await Promise.resolve();
    });

    expect(
      screen.getByRole("status", { name: "Состояние карты" }),
    ).toHaveTextContent("Карта готова");
    expect(
      screen.getByRole("alert", { name: "Ошибка данных событий" }),
    ).toHaveTextContent("геометрия остается нейтральной");
    expect(screen.getByRole("main")).not.toHaveTextContent(
      "raw snapshot reason",
    );
    expect(screen.getByRole("main")).not.toHaveTextContent("0 событий");
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(1);
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(2);
  });

  test("Roster mismatch отклоняет snapshot целиком, не создавая второй source или динамический layer", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await flushSnapshotStart();
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createMismatchedCountryGeometry(),
      );
      requireSnapshotRequest(snapshotFixture).resolve(
        createValidatedCountrySnapshot(),
      );
      await Promise.resolve();
    });

    expect(
      screen.getByRole("alert", { name: "Ошибка данных событий" }),
    ).toHaveTextContent("Не удалось загрузить, проверить или связать");
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(1);
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(2);
  });

  test.each(["PARTIAL", "UNKNOWN"] satisfies CountryCoverageStatus[])(
    "Показывает принятый %s с точным периодом и mapped count",
    async (coverageStatus) => {
      const mapFixture = createControlledMapLibreFactory();
      const geometryFixture = createControlledGeometryLoader();
      const snapshotFixture = createControlledSnapshotLoader();
      render(
        <CountryMapScreen
          geometryLoader={geometryFixture.loader}
          snapshotLoader={snapshotFixture.loader}
          snapshotClock={() => new Date("2026-08-11T12:16:30Z")}
          mapFactory={mapFixture.factory}
        />,
      );

      await flushSnapshotStart();
      await act(async () => {
        requireGeometryRequest(geometryFixture).resolve(
          createValidatedCountryGeometry(),
        );
        requireSnapshotRequest(snapshotFixture).resolve(
          createSnapshotWithCoverage(coverageStatus),
        );
        await Promise.resolve();
      });

      expect(
        screen.getByRole("status", { name: "Состояние данных событий" }),
      ).toHaveTextContent(`Полнота: ${coverageStatus}`);
      expect(
        screen.getByRole("status", { name: "Состояние данных событий" }),
      ).toHaveTextContent("Сопоставлено событий: 4");
      expect(
        screen.queryByRole("alert", { name: "Ошибка данных событий" }),
      ).not.toBeInTheDocument();
    },
  );

  test("Refresh warning сохраняет период и время последнего принятия", async () => {
    vi.useFakeTimers();
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        snapshotClock={() => new Date("2026-08-11T12:16:30Z")}
        snapshotRefreshIntervalMilliseconds={1_000}
        mapFactory={mapFixture.factory}
      />,
    );

    await flushSnapshotStart();
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      requireSnapshotRequest(snapshotFixture, 0).resolve(
        createValidatedCountrySnapshot(),
      );
      await Promise.resolve();
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_000);
    });
    expect(
      screen.getByRole("status", {
        name: "Состояние обновления данных событий",
      }),
    ).toHaveTextContent("Последние принятые данные остаются доступными");

    await act(async () => {
      requireSnapshotRequest(snapshotFixture, 1).reject(new Error("503"));
      await Promise.resolve();
    });

    expect(
      screen.getByRole("alert", {
        name: "Предупреждение обновления данных событий",
      }),
    ).toHaveTextContent("Показан последний успешно принятый снимок");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Время принятия: 2026-08-11T12:16:30.000Z");
  });

  test("Strict Mode освобождает первый lifecycle и оставляет второй экран готовым", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <StrictMode>
        <CountryMapScreen
          geometryLoader={geometryFixture.loader}
          snapshotLoader={snapshotFixture.loader}
          mapFactory={mapFixture.factory}
        />
      </StrictMode>,
    );

    const firstGeometryRequest = requireGeometryRequest(geometryFixture, 0);
    const secondGeometryRequest = requireGeometryRequest(geometryFixture, 1);
    const firstMap = requireControlledMap(mapFixture, 0);
    const secondMap = requireControlledMap(mapFixture, 1);

    expect(firstGeometryRequest.signal.aborted).toBe(true);
    expect(secondGeometryRequest.signal.aborted).toBe(false);
    expect(firstMap.removeCalls).toBe(1);

    await act(async () => {
      await Promise.resolve();
    });
    expect(snapshotFixture.requests).toHaveLength(1);
    const snapshotRequest = requireSnapshotRequest(snapshotFixture);
    expect(snapshotRequest.signal.aborted).toBe(false);

    act(() => {
      secondMap.emit("load");
    });
    await act(async () => {
      secondGeometryRequest.resolve(createValidatedCountryGeometry());
      snapshotRequest.resolve(createValidatedCountrySnapshot());
      await Promise.resolve();
    });

    expect(
      screen.getByRole("status", { name: "Состояние карты" }),
    ).toHaveTextContent("Карта готова");
    expect(
      screen.getByRole("status", { name: "Состояние данных событий" }),
    ).toHaveTextContent("Принят снимок событий");
    expect(secondMap.sourceAdditions).toHaveLength(1);
    expect(secondMap.layerAdditions).toHaveLength(2);
  });

  test("Принятый snapshot пока не добавляет легенду, выбор страны или управляющие элементы", async () => {
    const mapFixture = createControlledMapLibreFactory();
    const geometryFixture = createControlledGeometryLoader();
    const snapshotFixture = createControlledSnapshotLoader();
    render(
      <CountryMapScreen
        geometryLoader={geometryFixture.loader}
        snapshotLoader={snapshotFixture.loader}
        mapFactory={mapFixture.factory}
      />,
    );

    act(() => {
      requireControlledMap(mapFixture).emit("load");
    });
    await flushSnapshotStart();
    await act(async () => {
      requireGeometryRequest(geometryFixture).resolve(
        createValidatedCountryGeometry(),
      );
      requireSnapshotRequest(snapshotFixture).resolve(
        createValidatedCountrySnapshot(),
      );
      await Promise.resolve();
    });

    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("list")).not.toBeInTheDocument();
    expect(screen.queryByText(/легенд/i)).not.toBeInTheDocument();
    expect(requireControlledMap(mapFixture).sourceAdditions).toHaveLength(1);
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(2);
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

function createControlledSnapshotLoader(): ControlledSnapshotLoader {
  const requests: ControlledSnapshotRequest[] = [];
  const loader: CountrySnapshotLoader = (signal) =>
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
    throw new Error(`Не найден тестовый geometry request с индексом ${index}`);
  }
  return request;
}

function requireSnapshotRequest(
  fixture: ControlledSnapshotLoader,
  index = 0,
): ControlledSnapshotRequest {
  const request = fixture.requests[index];
  if (request === undefined) {
    throw new Error(`Не найден тестовый snapshot request с индексом ${index}`);
  }
  return request;
}

function createSnapshotWithCoverage(
  coverageStatus: CountryCoverageStatus,
): CountrySnapshot {
  const document = createCountrySnapshotDocument();
  document.coverage.status = coverageStatus;
  document.coverage.missingIntervals =
    coverageStatus === "UNKNOWN"
      ? null
      : coverageStatus === "PARTIAL"
        ? [
            {
              from: "2026-08-10T12:15:00Z",
              to: "2026-08-10T12:30:00Z",
            },
          ]
        : [];
  return validateCountrySnapshot(document);
}

function createMismatchedCountryGeometry(): CountryGeometry {
  const document = createCountryGeometryDocument();
  const firstFeature = document.features[0];
  if (firstFeature === undefined) {
    throw new Error("Тестовая geometry не содержит первый регион");
  }
  firstFeature.properties.regionId = "country:different";
  return validateCountryGeometry(document);
}

async function flushSnapshotStart(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
  });
}
