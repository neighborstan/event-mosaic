import { describe, expect, test } from "vitest";

import { resolveBasemapProvider } from "./basemapProvider";

describe("Выбор источника фоновой карты", () => {
  test("Без явной настройки выбирает OpenFreeMap с видимой атрибуцией", () => {
    expect(resolveBasemapProvider(undefined)).toEqual({
      status: "resolved",
      configuration: {
        provider: "openfreemap",
        styleUrl: "https://tiles.openfreemap.org/styles/liberty",
        attributionControl: {
          compact: false,
        },
      },
    });
  });

  test("Явная настройка выбирает закрепленный стиль Stadia", () => {
    expect(resolveBasemapProvider("stadia")).toEqual({
      status: "resolved",
      configuration: {
        provider: "stadia",
        styleUrl: "https://tiles.stadiamaps.com/styles/alidade_smooth.json",
        attributionControl: {
          compact: false,
        },
      },
    });
  });

  test("Неизвестная настройка возвращает безопасную ошибку без исходного значения", () => {
    const resolution = resolveBasemapProvider("private-provider-value");

    expect(resolution).toEqual({
      status: "invalid",
      error: {
        code: "unsupported-basemap-provider",
        message:
          "Источник фоновой карты настроен неверно. Доступны openfreemap и stadia.",
      },
    });
    expect(JSON.stringify(resolution)).not.toContain("private-provider-value");
  });
});
