import { describe, expect, test } from "vitest";
import { createValidatedCountryGeometry } from "../../../test/countryGeometryFixtures";
import { createJoinedCountrySnapshot } from "../../../test/countrySnapshotFixtures";
import type { JoinedCountrySnapshotRegion } from "../api/countrySnapshot";
import {
  buildCountryVisualRegion,
  buildCountryVisualModel,
  COUNTRY_TONE_SUMMARIES,
  COUNTRY_EMPTY_COLOR,
  COUNTRY_MISSING_TONE_COLOR,
} from "./countryVisualModel";

describe("Понятная сводная окраска стран", () => {
  test.each([
    [92, 1, 7, "negative"],
    [80, 0, 20, "negative"],
    [79, 1, 20, "mostlyNegative"],
    [60, 0, 40, "mostlyNegative"],
    [59, 0, 41, "mixed"],
    [52, 4, 44, "mixed"],
    [50, 0, 50, "mixed"],
    [31, 0, 69, "mostlyPositive"],
    [40, 0, 60, "mostlyPositive"],
    [20, 0, 80, "positive"],
    [20, 60, 20, "zero"],
    [20, 59, 21, "mixed"],
  ] as const)(
    "При долях %i/%i/%i выбирает состояние %s",
    (negative, zero, positive, summaryKey) => {
      const result = buildCountryVisualRegion(
        createJoinedRegion("country:test", [
          negative,
          0,
          0,
          zero,
          positive,
          0,
          0,
        ]),
      );
      expect(result).toMatchObject({
        status: "tone-mixture",
        summaryKey,
        fillColor: COUNTRY_TONE_SUMMARIES[summaryKey].color,
      });
    },
  );

  test("Равная смесь положительных и отрицательных не выглядит как настоящий нулевой тон", () => {
    const mixed = buildCountryVisualRegion(
      createJoinedRegion("country:mixed", [50, 0, 0, 0, 50, 0, 0]),
    );
    const zero = buildCountryVisualRegion(
      createJoinedRegion("country:zero", [0, 0, 0, 100, 0, 0, 0]),
    );
    expect(mixed.fillColor).not.toBe(zero.fillColor);
    expect(mixed).toMatchObject({
      negativePercentage: 50,
      positivePercentage: 50,
      zeroPercentage: 0,
    });
  });

  test("Семь фактических долей не усиливаются и не включают неизвестный тон", () => {
    const region = createJoinedRegion("country:test", [1, 2, 3, 4, 5, 6, 7], 2);
    const result = buildCountryVisualRegion(region);
    expect(result.data).toBe(region.data);
    expect(result.groups.map((group) => group.count)).toEqual([
      1, 2, 3, 4, 5, 6, 7,
    ]);
    expect(result.groups[0]?.percentage).toBeCloseTo(100 / 28);
    expect(result.groups[6]?.percentage).toBe(25);
    expect(
      result.groups.reduce((sum, group) => sum + group.percentage, 0),
    ).toBeCloseTo(100);
  });

  test("Количество не меняет цвет тональности и порядок сравнения стран", () => {
    const low = buildCountryVisualRegion(
      createJoinedRegion("country:one", [8, 0, 0, 0, 2, 0, 0]),
    );
    const high = buildCountryVisualRegion(
      createJoinedRegion("country:many", [8000, 0, 0, 0, 2000, 0, 0]),
    );
    expect(low.fillColor).toBe(high.fillColor);
  });

  test.each([
    [1, "#d6e9ef"],
    [9, "#d6e9ef"],
    [10, "#a4cadb"],
    [99, "#a4cadb"],
    [100, "#6ca6c1"],
    [999, "#6ca6c1"],
    [1000, "#377e9e"],
    [9999, "#377e9e"],
    [10000, "#18516f"],
  ])("Количество %i относится к своему интервалу", (count, color) => {
    const result = buildCountryVisualRegion(
      createJoinedRegion("country:volume", [Number(count), 0, 0, 0, 0, 0, 0]),
      "volume",
    );
    expect(result.fillColor).toBe(color);
  });

  test("Отсутствие событий и неизвестный тон различимы, количество включает неизвестный тон", () => {
    const empty = buildCountryVisualRegion(
      createJoinedRegion("country:empty", [0, 0, 0, 0, 0, 0, 0]),
    );
    const unknown = createJoinedRegion(
      "country:missing",
      [0, 0, 0, 0, 0, 0, 0],
      50,
    );
    expect(empty).toMatchObject({
      status: "no-events",
      fillColor: COUNTRY_EMPTY_COLOR,
    });
    expect(buildCountryVisualRegion(unknown)).toMatchObject({
      status: "missing-tone-only",
      fillColor: COUNTRY_MISSING_TONE_COLOR,
    });
    expect(buildCountryVisualRegion(unknown, "volume")).toMatchObject({
      status: "missing-tone-only",
      fillColor: "#a4cadb",
    });
  });

  test("Повторное построение не меняет данные и сохраняет весь список стран", () => {
    const snapshot = createJoinedCountrySnapshot();
    const first = buildCountryVisualModel(snapshot);
    expect(first).toHaveLength(258);
    expect(buildCountryVisualModel(snapshot)).toEqual(first);
    expect(
      buildCountryVisualModel(snapshot, "volume").map((region) => region.data),
    ).toEqual(first.map((region) => region.data));
  });
});

function createJoinedRegion(
  regionId: string,
  counts: readonly [number, number, number, number, number, number, number],
  missingToneEventCount = 0,
): JoinedCountrySnapshotRegion {
  const geometry = requireGeometryFeature();
  const coloredEventCount = counts.reduce((sum, count) => sum + count, 0);

  return Object.freeze({
    geometry: Object.freeze({
      ...geometry,
      properties: Object.freeze({
        ...geometry.properties,
        regionId,
      }),
    }),
    data: Object.freeze({
      regionId,
      eventCount: coloredEventCount + missingToneEventCount,
      coloredEventCount,
      missingToneEventCount,
      toneCounts: Object.freeze({
        NEGATIVE_EXTREME: counts[0],
        NEGATIVE_STRONG: counts[1],
        NEGATIVE_MILD: counts[2],
        ZERO: counts[3],
        POSITIVE_MILD: counts[4],
        POSITIVE_STRONG: counts[5],
        POSITIVE_EXTREME: counts[6],
      }),
    }),
  });
}

function requireGeometryFeature() {
  const feature = createValidatedCountryGeometry().features[0];
  if (feature === undefined) {
    throw new Error("Тестовая geometry не содержит первый регион");
  }
  return feature;
}
