import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode } from "react";
import { afterEach, describe, expect, test, vi } from "vitest";

import {
  createCountryGeometryDocument,
  createValidatedCountryGeometry,
} from "../../../test/countryGeometryFixtures";
import {
  createCountrySnapshotDocument,
  createValidatedCountrySnapshot,
  type MutableCountrySnapshotDocument,
  type MutableCountrySnapshotRegion,
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

describe("Экран карты с проверенным snapshot", () => {
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
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(3);
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
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(3);
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
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(3);
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
        screen.getByRole("status", {
          name: "Дополнительное состояние данных событий",
        }),
      ).toHaveTextContent(
        coverageStatus === "PARTIAL"
          ? "Известные пропуски UTC: [2026-08-10T12:15:00Z, 2026-08-10T12:30:00Z)"
          : "Полнота сейчас неизвестна; список пропусков недоступен",
      );
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
        name: "Дополнительное состояние данных событий",
      }),
    ).toHaveTextContent("последние принятые данные остаются видимыми");

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
    expect(secondMap.layerAdditions).toHaveLength(3);
  });

  test("Объясняет сводные цвета, смешанную картину и отличие отсутствующих данных от нулевого тона", async () => {
    const { mapFixture } = await renderAcceptedScreen();
    const legend = screen.getByRole("region", {
      name: "Шкала тональности",
    });
    expect(within(legend).getAllByRole("listitem")).toHaveLength(6);
    expect(legend).toHaveTextContent("80% и более отрицательных");
    expect(legend).toHaveTextContent("Преобладает тон 0");
    expect(legend).toHaveTextContent(
      "Смешанная картина не означает нулевой тон",
    );
    expect(legend).toHaveTextContent("Нет событий в доступных данных");
    expect(legend).toHaveTextContent("Тональность неизвестна");
    expect(requireControlledMap(mapFixture).layerAdditions).toHaveLength(3);
  });

  test("Выбор страны из списка и смена показателя сохраняют сводку без новых запросов и пересоздания карты", async () => {
    const user = userEvent.setup();
    const { geometry, mapFixture, geometryFixture, snapshotFixture } =
      await renderAcceptedScreen();
    const firstRegion = requireGeometryFeature(geometry, 0);
    const map = requireControlledMap(mapFixture);
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Страна" }),
      firstRegion.properties.regionId,
    );
    const summary = screen.getByRole("complementary", {
      name: firstRegion.properties.displayName,
    });
    const toneSummary = summary.textContent;
    const toneColor = map.stateFor(firstRegion.properties.regionId)[
      "fillColor"
    ];
    await user.click(
      screen.getByRole("button", { name: "Количество событий" }),
    );
    expect(
      screen.getByRole("button", { name: "Количество событий" }),
    ).toHaveAttribute("aria-pressed", "true");
    const legend = screen.getByRole("region", {
      name: "Шкала количества событий",
    });
    expect(within(legend).getAllByRole("listitem")).toHaveLength(5);
    expect(legend).toHaveTextContent(
      "включая события с неизвестной тональностью",
    );
    expect(summary.textContent).toBe(toneSummary);
    expect(map.stateFor(firstRegion.properties.regionId)["fillColor"]).not.toBe(
      toneColor,
    );
    await user.click(screen.getByRole("button", { name: "Тональность" }));
    expect(map.stateFor(firstRegion.properties.regionId)["fillColor"]).toBe(
      toneColor,
    );
    expect(map.stateFor(firstRegion.properties.regionId)["selected"]).toBe(
      true,
    );
    expect(geometryFixture.requests).toHaveLength(1);
    expect(snapshotFixture.requests).toHaveLength(1);
    expect(map.sourceAdditions).toHaveLength(1);
    expect(map.layerAdditions).toHaveLength(3);
    await user.keyboard("{Escape}");
    expect(screen.getByRole("combobox", { name: "Страна" })).toHaveValue("");
    expect(screen.queryByRole("complementary")).not.toBeInTheDocument();
  });

  test("Наведение не заменяет выбор, а новая страна, фон, кнопка закрытия и Escape управляют одной закрепленной сводкой", async () => {
    const user = userEvent.setup();
    const { geometry, mapFixture } = await renderAcceptedScreen();
    const firstRegion = requireGeometryFeature(geometry, 0);
    const secondRegion = requireGeometryFeature(geometry, 1);
    const map = requireControlledMap(mapFixture);

    act(() => {
      map.emitCountry("select", firstRegion.properties.regionId);
      map.emitCountry("hover", secondRegion.properties.regionId);
    });

    expect(
      screen.getByRole("complementary", {
        name: firstRegion.properties.displayName,
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("tooltip", { name: "Краткая сводка страны" }),
    ).toHaveTextContent(secondRegion.properties.displayName);

    act(() => {
      map.emitCountry("leave", null);
    });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(
      screen.getByRole("complementary", {
        name: firstRegion.properties.displayName,
      }),
    ).toBeInTheDocument();

    act(() => {
      map.emitCountry("select", secondRegion.properties.regionId);
    });
    expect(
      screen.queryByRole("complementary", {
        name: firstRegion.properties.displayName,
      }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("complementary", {
        name: secondRegion.properties.displayName,
      }),
    ).toBeInTheDocument();

    act(() => {
      map.emitBackgroundSelect();
    });
    expect(screen.queryByRole("complementary")).not.toBeInTheDocument();

    act(() => {
      map.emitCountry("select", firstRegion.properties.regionId);
    });
    await user.click(
      screen.getByRole("button", { name: "Закрыть сводку страны" }),
    );
    expect(screen.queryByRole("complementary")).not.toBeInTheDocument();

    act(() => {
      map.emitCountry("select", secondRegion.properties.regionId);
    });
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("complementary")).not.toBeInTheDocument();
    expect(mapFixture.maps).toHaveLength(1);
  });

  test("Выбор по regionId переживает наведение и обновляет факты после refresh", async () => {
    vi.useFakeTimers();
    const geometry = createValidatedCountryGeometry();
    const selectedFeature = requireGeometryFeature(geometry, 0);
    const hoveredFeature = requireGeometryFeature(geometry, 1);
    const { mapFixture, snapshotFixture } = await renderAcceptedScreen({
      geometry,
      snapshotRefreshIntervalMilliseconds: 1_000,
    });
    const map = requireControlledMap(mapFixture);

    act(() => {
      map.emitCountry("select", selectedFeature.properties.regionId);
      map.emitCountry("hover", hoveredFeature.properties.regionId);
    });
    expect(
      screen.getByRole("complementary", {
        name: selectedFeature.properties.displayName,
      }),
    ).toHaveTextContent("Событий: 4");
    expect(
      screen.getByRole("tooltip", { name: "Краткая сводка страны" }),
    ).toHaveTextContent(hoveredFeature.properties.displayName);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_000);
    });
    await act(async () => {
      requireSnapshotRequest(snapshotFixture, 1).resolve(
        createRefreshedCountrySnapshot(selectedFeature.properties.regionId),
      );
      await Promise.resolve();
    });

    const refreshedSummary = screen.getByRole("complementary", {
      name: selectedFeature.properties.displayName,
    });
    expect(refreshedSummary).toHaveTextContent("Событий: 8");
    expect(refreshedSummary).toHaveTextContent("С известной тональностью: 7");
    expect(
      screen.getByRole("tooltip", { name: "Краткая сводка страны" }),
    ).toHaveTextContent(hoveredFeature.properties.displayName);
    expect(mapFixture.maps).toHaveLength(1);
  });

  test.each([
    ["COMPLETE", "0 событий"],
    ["PARTIAL", "0 в доступных данных"],
    ["UNKNOWN", "0 в доступных данных"],
  ] satisfies readonly (readonly [CountryCoverageStatus, string])[])(
    "Нулевая страна при %s сообщает: %s",
    async (coverageStatus, expectedText) => {
      const geometry = createValidatedCountryGeometry();
      const emptyFeature = requireGeometryFeature(geometry, 2);
      const { mapFixture } = await renderAcceptedScreen({
        geometry,
        snapshot: createSnapshotWithCoverage(coverageStatus),
      });

      act(() => {
        requireControlledMap(mapFixture).emitCountry(
          "select",
          emptyFeature.properties.regionId,
        );
      });

      const summary = screen.getByRole("complementary", {
        name: emptyFeature.properties.displayName,
      });
      expect(summary).toHaveTextContent(expectedText);
      expect(summary).toHaveTextContent("С известной тональностью: 0");
      expect(summary).toHaveTextContent("Тональность неизвестна: 0");
      expect(summary).toHaveTextContent(
        `Полнота всего снимка: ${coverageStatus}`,
      );
    },
  );

  test("Неизвестная тональность, спорный статус и источник геометрии видны без приписывания стране общей статистики", async () => {
    const geometry = createValidatedCountryGeometry();
    const disputedFeature = requireGeometryFeature(geometry, 1);
    const { mapFixture } = await renderAcceptedScreen({
      geometry,
      snapshot: createMissingToneSnapshot(disputedFeature.properties.regionId),
    });

    act(() => {
      requireControlledMap(mapFixture).emitCountry(
        "select",
        disputedFeature.properties.regionId,
      );
    });

    const summary = screen.getByRole("complementary", {
      name: disputedFeature.properties.displayName,
    });
    expect(summary).toHaveTextContent(
      "Для всех событий тональность неизвестна",
    );
    expect(summary).toHaveTextContent("Тональность неизвестна: 2");
    expect(summary).toHaveTextContent(
      "Источник геометрии: Natural Earth 10m (natural-earth-10m), версия country-v1",
    );
    expect(summary).toHaveTextContent(
      "Статус: спорная территория в представлении de facto",
    );
    expect(summary).not.toHaveTextContent("Без надежного места действия: 3");
    expect(summary).not.toHaveTextContent("Не сопоставлено с границами: 6");
    expect(
      screen.getByRole("status", {
        name: "Дополнительное состояние данных событий",
      }),
    ).toHaveTextContent("Без надежного места действия: 3");
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

type RenderAcceptedScreenOptions = Readonly<{
  geometry?: CountryGeometry;
  snapshot?: CountrySnapshot;
  snapshotRefreshIntervalMilliseconds?: number;
}>;

async function renderAcceptedScreen({
  geometry = createValidatedCountryGeometry(),
  snapshot = createValidatedCountrySnapshot(),
  snapshotRefreshIntervalMilliseconds,
}: RenderAcceptedScreenOptions = {}) {
  const mapFixture = createControlledMapLibreFactory();
  const geometryFixture = createControlledGeometryLoader();
  const snapshotFixture = createControlledSnapshotLoader();
  render(
    <CountryMapScreen
      geometryLoader={geometryFixture.loader}
      snapshotLoader={snapshotFixture.loader}
      snapshotClock={() => new Date("2026-08-11T12:16:30Z")}
      snapshotRefreshIntervalMilliseconds={snapshotRefreshIntervalMilliseconds}
      mapFactory={mapFixture.factory}
    />,
  );

  await flushSnapshotStart();
  act(() => {
    requireControlledMap(mapFixture).emit("load");
  });
  await act(async () => {
    requireGeometryRequest(geometryFixture).resolve(geometry);
    requireSnapshotRequest(snapshotFixture).resolve(snapshot);
    await Promise.resolve();
  });

  return {
    geometry,
    geometryFixture,
    mapFixture,
    snapshotFixture,
  };
}

function requireGeometryFeature(
  geometry: CountryGeometry,
  index: number,
): CountryGeometry["features"][number] {
  const feature = geometry.features[index];
  if (feature === undefined) {
    throw new Error(`Не найден тестовый регион с индексом ${index}`);
  }
  return feature;
}

function createRefreshedCountrySnapshot(regionId: string): CountrySnapshot {
  const document = createCountrySnapshotDocument();
  const region = requireSnapshotRegion(document, regionId);
  const previousEventCount = region.eventCount;

  region.eventCount = 8;
  region.coloredEventCount = 7;
  region.missingToneEventCount = 1;
  region.toneCounts.NEGATIVE_EXTREME = 2;
  region.toneCounts.NEGATIVE_STRONG = 1;
  region.toneCounts.NEGATIVE_MILD = 1;
  region.toneCounts.ZERO = 1;
  region.toneCounts.POSITIVE_MILD = 1;
  region.toneCounts.POSITIVE_STRONG = 1;
  region.toneCounts.POSITIVE_EXTREME = 0;
  document.quality.mappedEventCount += region.eventCount - previousEventCount;
  document.quality.eligibleEventCount =
    document.quality.mappedEventCount +
    document.quality.unlocatedEventCount +
    document.quality.unmappedEventCount;
  return validateCountrySnapshot(document);
}

function createMissingToneSnapshot(regionId: string): CountrySnapshot {
  const document = createCountrySnapshotDocument();
  const originalPopulatedRegion = document.regions.find(
    (region) => region.eventCount > 0,
  );
  if (originalPopulatedRegion === undefined) {
    throw new Error("Тестовый snapshot не содержит заполненный регион");
  }
  const missingToneRegion = requireSnapshotRegion(document, regionId);
  if (missingToneRegion === originalPopulatedRegion) {
    throw new Error("Для missing tone нужен отдельный тестовый регион");
  }

  originalPopulatedRegion.eventCount = 2;
  originalPopulatedRegion.coloredEventCount = 2;
  originalPopulatedRegion.missingToneEventCount = 0;
  originalPopulatedRegion.toneCounts.NEGATIVE_EXTREME = 1;
  originalPopulatedRegion.toneCounts.NEGATIVE_STRONG = 0;
  originalPopulatedRegion.toneCounts.NEGATIVE_MILD = 0;
  originalPopulatedRegion.toneCounts.ZERO = 1;
  originalPopulatedRegion.toneCounts.POSITIVE_MILD = 0;
  originalPopulatedRegion.toneCounts.POSITIVE_STRONG = 0;
  originalPopulatedRegion.toneCounts.POSITIVE_EXTREME = 0;

  missingToneRegion.eventCount = 2;
  missingToneRegion.coloredEventCount = 0;
  missingToneRegion.missingToneEventCount = 2;
  missingToneRegion.toneCounts.NEGATIVE_EXTREME = 0;
  missingToneRegion.toneCounts.NEGATIVE_STRONG = 0;
  missingToneRegion.toneCounts.NEGATIVE_MILD = 0;
  missingToneRegion.toneCounts.ZERO = 0;
  missingToneRegion.toneCounts.POSITIVE_MILD = 0;
  missingToneRegion.toneCounts.POSITIVE_STRONG = 0;
  missingToneRegion.toneCounts.POSITIVE_EXTREME = 0;
  return validateCountrySnapshot(document);
}

function requireSnapshotRegion(
  document: MutableCountrySnapshotDocument,
  regionId: string,
): MutableCountrySnapshotRegion {
  const region = document.regions.find(
    (candidate) => candidate.regionId === regionId,
  );
  if (region === undefined) {
    throw new Error(`Не найден тестовый snapshot region ${regionId}`);
  }
  return region;
}

async function flushSnapshotStart(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
  });
}
