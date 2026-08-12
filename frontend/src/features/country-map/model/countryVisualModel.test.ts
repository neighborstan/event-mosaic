import { describe, expect, test } from "vitest";

import { createValidatedCountryGeometry } from "../../../test/countryGeometryFixtures";
import { createJoinedCountrySnapshot } from "../../../test/countrySnapshotFixtures";
import type { CountrySnapshotRegion } from "../api/countrySnapshot";
import type { JoinedCountrySnapshotRegion } from "../api/countrySnapshot";
import {
  COUNTRY_TONE_LEGEND_ITEMS,
  COUNTRY_TONE_VISUAL_CONFIGURATION,
  buildCountryVisualModel,
  buildCountryVisualRegion,
  type CountryTonePattern,
} from "./countryVisualModel";

describe("Визуальная модель тональности стран", () => {
  test("Сохраняет точный порядок, палитру и параметры конфигурации", () => {
    expect(COUNTRY_TONE_VISUAL_CONFIGURATION).toEqual({
      configurationId: "tone-bands-v1-soft-grain-v1",
      toneModelVersion: "tone-bands-v1",
      groupOrder: [
        "NEGATIVE_EXTREME",
        "NEGATIVE_STRONG",
        "NEGATIVE_MILD",
        "ZERO",
        "POSITIVE_MILD",
        "POSITIVE_STRONG",
        "POSITIVE_EXTREME",
      ],
      palette: {
        NEGATIVE_EXTREME: "#7b1f24",
        NEGATIVE_STRONG: "#bd3a2b",
        NEGATIVE_MILD: "#e9834b",
        ZERO: "#929ba5",
        POSITIVE_MILD: "#64bec3",
        POSITIVE_STRONG: "#177f99",
        POSITIVE_EXTREME: "#174f78",
      },
      visualWeight: "sqrt-count",
      volumeScale: {
        referenceColoredEventCount: 1_000,
        densityMinimum: 0.1,
        densityRange: 0.66,
        densityMaximum: 0.76,
        opacityMinimum: 0.34,
        opacityRange: 0.4,
        opacityMaximum: 0.74,
      },
      texture: {
        width: 64,
        height: 64,
        grainRadius: 1.75,
        pixelExtent: 3,
        minimumUncoveredFraction: 0.02,
        fnvOffsetBasis: 2_166_136_261,
        fnvPrime: 16_777_619,
        zeroSeedReplacement: 0x9e37_79b9,
      },
    });
    expect(
      COUNTRY_TONE_LEGEND_ITEMS.map(({ key, color, range }) => ({
        key,
        color,
        range,
      })),
    ).toEqual([
      {
        key: "NEGATIVE_EXTREME",
        color: "#7b1f24",
        range: "averageTone <= -8",
      },
      {
        key: "NEGATIVE_STRONG",
        color: "#bd3a2b",
        range: "-8 < averageTone <= -3",
      },
      {
        key: "NEGATIVE_MILD",
        color: "#e9834b",
        range: "-3 < averageTone < 0",
      },
      { key: "ZERO", color: "#929ba5", range: "averageTone = 0" },
      {
        key: "POSITIVE_MILD",
        color: "#64bec3",
        range: "0 < averageTone < 3",
      },
      {
        key: "POSITIVE_STRONG",
        color: "#177f99",
        range: "3 <= averageTone < 8",
      },
      {
        key: "POSITIVE_EXTREME",
        color: "#174f78",
        range: "averageTone >= 8",
      },
    ]);
  });

  test("Совпадает с нормативным fingerprint и RGBA golden vector", async () => {
    const pattern = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:usa", [16, 25, 36, 49, 64, 81, 100]),
      ),
    );

    expect(pattern.fingerprint).toBe(
      '["tone-bands-v1-soft-grain-v1","country:usa",16,25,36,49,64,81,100]',
    );
    await expect(sha256Hex(pattern.fingerprint)).resolves.toBe(
      "3bbbadeff5308850f94b1d9bd6cece6ec9e7d6a02448c856e85616a68a87d1a0",
    );
    expect(pattern.seed).toBe(321_884_420);
    expect(pattern.volume).toBe(0.6090976933136424);
    expect(pattern.density).toBe(0.502004477587004);
    expect(pattern.opacity).toBe(0.583639077325457);
    expect(pattern.grainCount).toBe(297);
    expect(pattern.rgba).toHaveLength(16_384);
    await expect(sha256Hex(pattern.rgba)).resolves.toBe(
      "43dc4ec1dd98fddf98592438e7743feda0d8ba133309e97e94e5d333d81a59c8",
    );
  });

  test("Повторяет одинаковые bytes и меняет узор вместе с regionId", async () => {
    const first = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:repeat", [1, 2, 3, 4, 5, 6, 7]),
      ),
    );
    const repeated = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:repeat", [1, 2, 3, 4, 5, 6, 7]),
      ),
    );
    const anotherRegion = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:another", [1, 2, 3, 4, 5, 6, 7]),
      ),
    );

    expect(first.rgba).not.toBe(repeated.rgba);
    expect(first.fingerprint).toBe(repeated.fingerprint);
    expect(first.rgba).toEqual(repeated.rgba);
    expect(anotherRegion.fingerprint).not.toBe(first.fingerprint);
    expect(await sha256Hex(anotherRegion.rgba)).not.toBe(
      await sha256Hex(first.rgba),
    );
  });

  test("Сохраняет видимым один Event и монотонно ограничивает объем", () => {
    const oneEvent = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:one", [1, 0, 0, 0, 0, 0, 0]),
      ),
    );
    const typical = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:typical", [100, 0, 0, 0, 0, 0, 0]),
      ),
    );
    const saturated = requireTonePattern(
      buildCountryVisualRegion(
        createJoinedRegion("country:saturated", [1_500, 0, 0, 0, 0, 0, 0]),
      ),
    );

    expect(oneEvent.volume).toBeGreaterThan(0);
    expect(oneEvent.density).toBeGreaterThan(0.1);
    expect(oneEvent.opacity).toBeGreaterThan(0.34);
    expect(oneEvent.grainCount).toBeGreaterThan(0);
    expect(oneEvent.rgba.some((byte) => byte !== 0)).toBe(true);
    expect(typical.density).toBeGreaterThan(oneEvent.density);
    expect(typical.opacity).toBeGreaterThan(oneEvent.opacity);
    expect(saturated.volume).toBe(1);
    expect(saturated.density).toBe(0.76);
    expect(saturated.opacity).toBe(0.74);
    expect(saturated.density).toBeGreaterThanOrEqual(typical.density);
    expect(saturated.opacity).toBeGreaterThanOrEqual(typical.opacity);
  });

  test("Отличает настоящий ZERO от отсутствующей тональности и отсутствия событий", () => {
    const factualZero = buildCountryVisualRegion(
      createJoinedRegion("country:zero", [0, 0, 0, 7, 0, 0, 0]),
    );
    const missingToneOnly = buildCountryVisualRegion(
      createJoinedRegion("country:missing", [0, 0, 0, 0, 0, 0, 0], 4),
    );
    const noEvents = buildCountryVisualRegion(
      createJoinedRegion("country:empty", [0, 0, 0, 0, 0, 0, 0]),
    );

    expect(factualZero.status).toBe("tone-mixture");
    expect(requireGroup(factualZero, "ZERO")).toMatchObject({
      count: 7,
      percentage: 100,
      color: "#929ba5",
    });
    expect(missingToneOnly.status).toBe("missing-tone-only");
    expect(missingToneOnly.data.eventCount).toBe(4);
    expect(missingToneOnly.data.missingToneEventCount).toBe(4);
    expect(missingToneOnly.groups.every((group) => group.count === 0)).toBe(
      true,
    );
    expect(noEvents.status).toBe("no-events");
    expect(noEvents.data.eventCount).toBe(0);
  });

  test("Показывает линейные проценты отдельно от корневых visual weights", () => {
    const visualRegion = buildCountryVisualRegion(
      createJoinedRegion("country:percent", [1, 2, 3, 4, 5, 6, 7], 2),
    );
    const percentages = visualRegion.groups.map((group) => group.percentage);

    expect(visualRegion.data.eventCount).toBe(30);
    expect(visualRegion.data.coloredEventCount).toBe(28);
    expect(percentages).toEqual([
      (1 / 28) * 100,
      (2 / 28) * 100,
      (3 / 28) * 100,
      (4 / 28) * 100,
      (5 / 28) * 100,
      (6 / 28) * 100,
      (7 / 28) * 100,
    ]);
    expect(percentages.reduce((sum, percentage) => sum + percentage, 0)).toBe(
      100,
    );
    expect(visualRegion.groups.map((group) => group.visualWeight)).toEqual([
      1,
      Math.sqrt(2),
      Math.sqrt(3),
      2,
      Math.sqrt(5),
      Math.sqrt(6),
      Math.sqrt(7),
    ]);
  });

  test("Строит неизменяемый список для всех регионов принятого snapshot", () => {
    const visualModel = buildCountryVisualModel(createJoinedCountrySnapshot());

    expect(visualModel).toHaveLength(258);
    expect(Object.isFrozen(visualModel)).toBe(true);
    expect(
      visualModel.filter((region) => region.status === "tone-mixture"),
    ).toHaveLength(1);
    expect(
      visualModel.filter((region) => region.status === "no-events"),
    ).toHaveLength(257);
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

function requireTonePattern(
  visualRegion: ReturnType<typeof buildCountryVisualRegion>,
): CountryTonePattern {
  if (visualRegion.status !== "tone-mixture") {
    throw new Error("Тестовый регион не получил tone pattern");
  }
  return visualRegion.pattern;
}

function requireGroup(
  visualRegion: ReturnType<typeof buildCountryVisualRegion>,
  key: CountrySnapshotRegion["toneCounts"] extends infer ToneCounts
    ? keyof ToneCounts
    : never,
) {
  const group = visualRegion.groups.find((candidate) => candidate.key === key);
  if (group === undefined) {
    throw new Error(`Тестовая модель не содержит группу ${String(key)}`);
  }
  return group;
}

async function sha256Hex(
  value: string | Uint8Array<ArrayBuffer>,
): Promise<string> {
  const bytes =
    typeof value === "string" ? new TextEncoder().encode(value) : value;
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
