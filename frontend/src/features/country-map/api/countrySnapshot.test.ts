import { describe, expect, test, vi } from "vitest";

import {
  createCountryGeometryDocument,
  createValidatedCountryGeometry,
} from "../../../test/countryGeometryFixtures";
import { createCountrySnapshotDocument } from "../../../test/countrySnapshotFixtures";
import { validateCountryGeometry } from "./countryGeometry";
import {
  COUNTRY_SNAPSHOT_URL,
  CountrySnapshotJoinError,
  CountrySnapshotValidationError,
  joinCountrySnapshot,
  loadCountrySnapshot,
  validateCountrySnapshot,
  type CountrySnapshotFetcher,
} from "./countrySnapshot";

describe("Безопасная граница country snapshot", () => {
  test("Загружает неизменный endpoint без browser cache и принимает весь tone-bands-v1 документ", async () => {
    const document = createCountrySnapshotDocument();
    const fetcher = vi
      .fn<CountrySnapshotFetcher>()
      .mockResolvedValue(
        jsonResponse(document, "application/json; charset=UTF-8"),
      );
    const controller = new AbortController();

    const snapshot = await loadCountrySnapshot({
      signal: controller.signal,
      fetcher,
    });

    expect(fetcher).toHaveBeenCalledOnce();
    expect(fetcher).toHaveBeenCalledWith(COUNTRY_SNAPSHOT_URL, {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
      signal: controller.signal,
    });
    expect(snapshot.snapshot).toEqual({
      from: "2026-08-10T12:15:00Z",
      to: "2026-08-11T12:15:00Z",
      geometryVersion: "country-v1",
      toneModelVersion: "tone-bands-v1",
    });
    expect(snapshot.regions).toHaveLength(258);
    expect(snapshot.regions[0]?.toneCounts).toEqual({
      NEGATIVE_EXTREME: 1,
      NEGATIVE_STRONG: 0,
      NEGATIVE_MILD: 0,
      ZERO: 1,
      POSITIVE_MILD: 0,
      POSITIVE_STRONG: 0,
      POSITIVE_EXTREME: 1,
    });
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.regions)).toBe(true);
  });

  test("Связывает те же 258 regionId взаимно однозначно независимо от порядка geometry", () => {
    const snapshot = validateCountrySnapshot(createCountrySnapshotDocument());
    const geometry = createValidatedCountryGeometry();

    const joined = joinCountrySnapshot(snapshot, geometry);

    expect(joined.regions).toHaveLength(258);
    expect(
      joined.regions.every(
        ({ geometry: feature, data }) =>
          feature.properties.regionId === data.regionId,
      ),
    ).toBe(true);
  });

  test("Отклоняет sign-v1, другую geometry version, неизвестное поле и небезопасный вложенный счетчик целиком", () => {
    const signDocument = createCountrySnapshotDocument();
    signDocument.snapshot.toneModelVersion = "sign-v1";
    expect(() => validateCountrySnapshot(signDocument)).toThrowError(
      CountrySnapshotValidationError,
    );

    const versionDocument = createCountrySnapshotDocument();
    versionDocument.snapshot.geometryVersion = "country-v2";
    expect(() => validateCountrySnapshot(versionDocument)).toThrowError(
      CountrySnapshotValidationError,
    );

    const unknownFieldDocument = createCountrySnapshotDocument();
    Object.assign(unknownFieldDocument.quality, { internalReason: "secret" });
    expect(() => validateCountrySnapshot(unknownFieldDocument)).toThrowError(
      CountrySnapshotValidationError,
    );

    const unsafeDocument = createCountrySnapshotDocument();
    const firstRegion = unsafeDocument.regions[0];
    if (firstRegion === undefined) {
      throw new Error("Тестовый snapshot не содержит первый регион");
    }
    firstRegion.toneCounts.NEGATIVE_EXTREME = Number.MAX_SAFE_INTEGER + 1;
    expect(() => validateCountrySnapshot(unsafeDocument)).toThrowError(
      CountrySnapshotValidationError,
    );
  });

  test("Отклоняет нарушенные суммы, форму coverage и неполный roster", () => {
    const brokenEquality = createCountrySnapshotDocument();
    brokenEquality.quality.eligibleEventCount += 1;
    expect(() => validateCountrySnapshot(brokenEquality)).toThrowError(
      CountrySnapshotValidationError,
    );

    const brokenCoverage = createCountrySnapshotDocument();
    brokenCoverage.coverage.status = "PARTIAL";
    expect(() => validateCountrySnapshot(brokenCoverage)).toThrowError(
      CountrySnapshotValidationError,
    );

    const incompleteRoster = createCountrySnapshotDocument();
    incompleteRoster.regions.pop();
    expect(() => validateCountrySnapshot(incompleteRoster)).toThrowError(
      CountrySnapshotValidationError,
    );
  });

  test("Не связывает snapshot с другим roster даже при той же версии", () => {
    const geometryDocument = createCountryGeometryDocument();
    const firstFeature = geometryDocument.features[0];
    if (firstFeature === undefined) {
      throw new Error("Тестовая geometry не содержит первый регион");
    }
    firstFeature.properties.regionId = "country:different";
    const geometry = validateCountryGeometry(geometryDocument);
    const snapshot = validateCountrySnapshot(createCountrySnapshotDocument());

    expect(() => joinCountrySnapshot(snapshot, geometry)).toThrowError(
      CountrySnapshotJoinError,
    );
  });

  test("Не раскрывает HTTP, media type и malformed JSON за безопасной ошибкой", async () => {
    const controller = new AbortController();
    const httpFetcher = vi
      .fn<CountrySnapshotFetcher>()
      .mockResolvedValue(new Response(null, { status: 503 }));
    await expect(
      loadCountrySnapshot({ signal: controller.signal, fetcher: httpFetcher }),
    ).rejects.toMatchObject({
      code: "country-snapshot-http-error",
    });

    const mediaFetcher = vi
      .fn<CountrySnapshotFetcher>()
      .mockResolvedValue(
        jsonResponse(createCountrySnapshotDocument(), "text/plain"),
      );
    await expect(
      loadCountrySnapshot({ signal: controller.signal, fetcher: mediaFetcher }),
    ).rejects.toMatchObject({
      code: "country-snapshot-media-type-error",
    });

    const malformedFetcher = vi.fn<CountrySnapshotFetcher>().mockResolvedValue(
      new Response("{", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await expect(
      loadCountrySnapshot({
        signal: controller.signal,
        fetcher: malformedFetcher,
      }),
    ).rejects.toMatchObject({
      code: "country-snapshot-json-error",
    });
  });
});

function jsonResponse(document: unknown, contentType: string): Response {
  return new Response(JSON.stringify(document), {
    status: 200,
    headers: { "Content-Type": contentType },
  });
}
