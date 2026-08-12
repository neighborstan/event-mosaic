import { describe, expect, test } from "vitest";

import {
  COUNTRY_GEOMETRY_URL,
  CountryGeometryLoadError,
  CountryGeometryValidationError,
  loadCountryGeometry,
  validateCountryGeometry,
  type CountryGeometryFetcher,
} from "./countryGeometry";
import { createCountryGeometryDocument } from "../../../test/countryGeometryFixtures";

describe("Проверка опубликованной геометрии стран", () => {
  test("Принимает целиком 258 уникальных Polygon и MultiPolygon регионов", () => {
    const document = createCountryGeometryDocument();

    const geometry = validateCountryGeometry(document);

    expect(geometry.geometryVersion).toBe("country-v1");
    expect(geometry.features).toHaveLength(258);
    expect(geometry.features[0]?.geometry.type).toBe("Polygon");
    expect(geometry.features[1]?.geometry.type).toBe("MultiPolygon");
    expect(Object.isFrozen(geometry)).toBe(true);
    expect(Object.isFrozen(geometry.features)).toBe(true);
    expect(Object.isFrozen(geometry.features[0]?.geometry.coordinates)).toBe(
      true,
    );
  });

  test.each([257, 259])(
    "Отклоняет документ с %i регионами вместо exact country-v1",
    (featureCount) => {
      expect(() =>
        validateCountryGeometry(createCountryGeometryDocument(featureCount)),
      ).toThrow(CountryGeometryValidationError);
    },
  );

  test.each([
    [
      "другую версию",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.geometryVersion = "country-v2";
      },
    ],
    [
      "не FeatureCollection",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.type = "GeometryCollection";
      },
    ],
    [
      "не Feature",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.type = "Geometry";
      },
    ],
    [
      "неподдерживаемый regionId",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.properties.regionId = "Country:RU";
      },
    ],
    [
      "повторный regionId",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[1]!.properties.regionId =
          document.features[0]!.properties.regionId;
      },
    ],
    [
      "неожиданное публичное свойство",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.properties.unexpectedProperty = "unsafe";
      },
    ],
    [
      "пустое отображаемое имя",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.properties.displayName = "   ";
      },
    ],
    [
      "неподдерживаемый disputed status",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.properties.disputeStatus = "UNKNOWN";
      },
    ],
    [
      "другой источник геометрии",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.properties.geometrySource = "client-generated";
      },
    ],
    [
      "неподдерживаемый тип геометрии",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.geometry.type = "Point";
      },
    ],
    [
      "незамкнутый Polygon ring",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.geometry.coordinates = [
          [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 1],
          ],
        ];
      },
    ],
    [
      "position лишней размерности",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.geometry.coordinates = [
          [
            [0, 0, 1],
            [1, 0],
            [1, 1],
            [0, 0, 1],
          ],
        ];
      },
    ],
    [
      "нечисловую координату",
      (document: ReturnType<typeof createCountryGeometryDocument>) => {
        document.features[0]!.geometry.coordinates = [
          [
            ["0", 0],
            [1, 0],
            [1, 1],
            ["0", 0],
          ],
        ];
      },
    ],
  ])("Отклоняет весь документ при нарушении: %s", (_name, mutate) => {
    const document = createCountryGeometryDocument();

    mutate(document);

    expect(() => validateCountryGeometry(document)).toThrow(
      CountryGeometryValidationError,
    );
  });

  test.each([
    [Number.NaN, 0],
    [Number.POSITIVE_INFINITY, 0],
    [-180.01, 0],
    [180.01, 0],
    [0, -90.01],
    [0, 90.01],
  ])(
    "Отклоняет не конечную или выходящую за диапазон position [%s, %s]",
    (longitude, latitude) => {
      const document = createCountryGeometryDocument();
      document.features[0]!.geometry.coordinates = [
        [
          [longitude, latitude],
          [1, 0],
          [1, 1],
          [longitude, latitude],
        ],
      ];

      expect(() => validateCountryGeometry(document)).toThrow(
        CountryGeometryValidationError,
      );
    },
  );
});

describe("Загрузка опубликованной геометрии стран", () => {
  test("Запрашивает exact root-relative country-v1 и принимает media type с параметрами без учета регистра", async () => {
    const controller = new AbortController();
    const calls: Readonly<{
      input: RequestInfo | URL;
      init: RequestInit | undefined;
    }>[] = [];
    const fetcher: CountryGeometryFetcher = (input, init) => {
      calls.push({ input, init });
      return Promise.resolve(
        geometryResponse(createCountryGeometryDocument(), {
          contentType: "Application/Geo+Json; charset=utf-8",
        }),
      );
    };

    const geometry = await loadCountryGeometry({
      signal: controller.signal,
      fetcher,
    });

    expect(geometry.features).toHaveLength(258);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.input).toBe(COUNTRY_GEOMETRY_URL);
    expect(calls[0]?.init?.method).toBe("GET");
    expect(new Headers(calls[0]?.init?.headers).get("accept")).toBe(
      "application/geo+json",
    );
    expect(calls[0]?.init?.signal).toBe(controller.signal);
  });

  test.each([
    [
      "неуспешный HTTP status",
      geometryResponse({}, { status: 503 }),
      "country-geometry-http-error",
    ],
    [
      "неверный media type",
      geometryResponse({}, { contentType: "application/json" }),
      "country-geometry-media-type-error",
    ],
    [
      "отсутствующий media type",
      geometryResponse({}),
      "country-geometry-media-type-error",
    ],
    [
      "поврежденный JSON",
      new Response("{", {
        headers: { "Content-Type": "application/geo+json" },
      }),
      "country-geometry-json-error",
    ],
    [
      "невалидную структуру",
      geometryResponse(
        { type: "FeatureCollection" },
        { contentType: "application/geo+json" },
      ),
      "country-geometry-validation-error",
    ],
  ])(
    "Возвращает безопасную ошибку при условии: %s",
    async (_name, response, code) => {
      const fetcher: CountryGeometryFetcher = () => Promise.resolve(response);

      await expect(
        loadCountryGeometry({
          signal: new AbortController().signal,
          fetcher,
        }),
      ).rejects.toMatchObject({ code });
    },
  );

  test("Не раскрывает сетевую ошибку как содержимое пользовательского состояния", async () => {
    const fetcher: CountryGeometryFetcher = () =>
      Promise.reject(new Error("https://private.example/token=secret"));

    await expect(
      loadCountryGeometry({
        signal: new AbortController().signal,
        fetcher,
      }),
    ).rejects.toEqual(
      new CountryGeometryLoadError("country-geometry-network-error"),
    );
  });

  test("Передает отмену AbortController без превращения в ошибку геометрии", async () => {
    const controller = new AbortController();
    const fetcher: CountryGeometryFetcher = (_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            reject(new DOMException("Aborted", "AbortError"));
          },
          { once: true },
        );
      });
    const request = loadCountryGeometry({
      signal: controller.signal,
      fetcher,
    });

    controller.abort();

    await expect(request).rejects.toMatchObject({ name: "AbortError" });
  });
});

function geometryResponse(
  body: unknown,
  options: Readonly<{
    status?: number;
    contentType?: string;
  }> = {},
): Response {
  const headers = new Headers();

  if (options.contentType !== undefined) {
    headers.set("Content-Type", options.contentType);
  }

  return new Response(JSON.stringify(body), {
    status: options.status ?? 200,
    headers,
  });
}
