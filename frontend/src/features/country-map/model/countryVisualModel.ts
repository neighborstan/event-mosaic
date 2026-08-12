import {
  COUNTRY_TONE_KEYS,
  type CountrySnapshotRegion,
  type CountryToneKey,
  type JoinedCountrySnapshot,
  type JoinedCountrySnapshotRegion,
} from "../api/countrySnapshot";
import type { CountryGeometryFeature } from "../api/countryGeometry";

export type CountryToneLegendItem = Readonly<{
  key: CountryToneKey;
  label: string;
  range: string;
  color: string;
}>;

export type CountryToneVisualGroup = Readonly<{
  key: CountryToneKey;
  label: string;
  color: string;
  count: number;
  percentage: number;
  visualWeight: number;
}>;

export type CountryTonePattern = Readonly<{
  fingerprint: string;
  rgba: Uint8Array<ArrayBuffer>;
  width: number;
  height: number;
  volume: number;
  density: number;
  opacity: number;
  seed: number;
  grainCount: number;
}>;

type CountryVisualRegionBase = Readonly<{
  regionId: string;
  geometry: CountryGeometryFeature;
  data: CountrySnapshotRegion;
  groups: readonly CountryToneVisualGroup[];
}>;

export type CountryVisualRegion =
  | (CountryVisualRegionBase & Readonly<{ status: "no-events" }>)
  | (CountryVisualRegionBase & Readonly<{ status: "missing-tone-only" }>)
  | (CountryVisualRegionBase &
      Readonly<{
        status: "tone-mixture";
        pattern: CountryTonePattern;
      }>);

export type CountryToneVisualConfiguration = Readonly<{
  configurationId: "tone-bands-v1-soft-grain-v1";
  toneModelVersion: "tone-bands-v1";
  groupOrder: readonly CountryToneKey[];
  palette: Readonly<Record<CountryToneKey, string>>;
  visualWeight: "sqrt-count";
  volumeScale: Readonly<{
    referenceColoredEventCount: 1_000;
    densityMinimum: 0.1;
    densityRange: 0.66;
    densityMaximum: 0.76;
    opacityMinimum: 0.34;
    opacityRange: 0.4;
    opacityMaximum: 0.74;
  }>;
  texture: Readonly<{
    width: 64;
    height: 64;
    grainRadius: 1.75;
    pixelExtent: 3;
    minimumUncoveredFraction: 0.02;
    fnvOffsetBasis: 2_166_136_261;
    fnvPrime: 16_777_619;
    zeroSeedReplacement: 2_654_435_769;
  }>;
}>;

const COUNTRY_TONE_PALETTE = Object.freeze({
  NEGATIVE_EXTREME: "#7b1f24",
  NEGATIVE_STRONG: "#bd3a2b",
  NEGATIVE_MILD: "#e9834b",
  ZERO: "#929ba5",
  POSITIVE_MILD: "#64bec3",
  POSITIVE_STRONG: "#177f99",
  POSITIVE_EXTREME: "#174f78",
}) satisfies Readonly<Record<CountryToneKey, string>>;

const COUNTRY_TONE_RGB = Object.freeze({
  NEGATIVE_EXTREME: Object.freeze([123, 31, 36] as const),
  NEGATIVE_STRONG: Object.freeze([189, 58, 43] as const),
  NEGATIVE_MILD: Object.freeze([233, 131, 75] as const),
  ZERO: Object.freeze([146, 155, 165] as const),
  POSITIVE_MILD: Object.freeze([100, 190, 195] as const),
  POSITIVE_STRONG: Object.freeze([23, 127, 153] as const),
  POSITIVE_EXTREME: Object.freeze([23, 79, 120] as const),
}) satisfies Readonly<
  Record<CountryToneKey, readonly [number, number, number]>
>;

const COUNTRY_TONE_LABELS = Object.freeze({
  NEGATIVE_EXTREME: "Крайне негативный",
  NEGATIVE_STRONG: "Сильно негативный",
  NEGATIVE_MILD: "Умеренно негативный",
  ZERO: "Тон ровно 0",
  POSITIVE_MILD: "Умеренно позитивный",
  POSITIVE_STRONG: "Сильно позитивный",
  POSITIVE_EXTREME: "Крайне позитивный",
}) satisfies Readonly<Record<CountryToneKey, string>>;

const COUNTRY_TONE_RANGES = Object.freeze({
  NEGATIVE_EXTREME: "averageTone <= -8",
  NEGATIVE_STRONG: "-8 < averageTone <= -3",
  NEGATIVE_MILD: "-3 < averageTone < 0",
  ZERO: "averageTone = 0",
  POSITIVE_MILD: "0 < averageTone < 3",
  POSITIVE_STRONG: "3 <= averageTone < 8",
  POSITIVE_EXTREME: "averageTone >= 8",
}) satisfies Readonly<Record<CountryToneKey, string>>;

export const COUNTRY_TONE_VISUAL_CONFIGURATION = Object.freeze({
  configurationId: "tone-bands-v1-soft-grain-v1",
  toneModelVersion: "tone-bands-v1",
  groupOrder: COUNTRY_TONE_KEYS,
  palette: COUNTRY_TONE_PALETTE,
  visualWeight: "sqrt-count",
  volumeScale: Object.freeze({
    referenceColoredEventCount: 1_000,
    densityMinimum: 0.1,
    densityRange: 0.66,
    densityMaximum: 0.76,
    opacityMinimum: 0.34,
    opacityRange: 0.4,
    opacityMaximum: 0.74,
  }),
  texture: Object.freeze({
    width: 64,
    height: 64,
    grainRadius: 1.75,
    pixelExtent: 3,
    minimumUncoveredFraction: 0.02,
    fnvOffsetBasis: 2_166_136_261,
    fnvPrime: 16_777_619,
    zeroSeedReplacement: 0x9e37_79b9,
  }),
}) satisfies CountryToneVisualConfiguration;

export const COUNTRY_TONE_LEGEND_ITEMS: readonly CountryToneLegendItem[] =
  Object.freeze(
    COUNTRY_TONE_KEYS.map((key) =>
      Object.freeze({
        key,
        label: COUNTRY_TONE_LABELS[key],
        range: COUNTRY_TONE_RANGES[key],
        color: COUNTRY_TONE_PALETTE[key],
      }),
    ),
  );

/** Строит чистую visual model одного уже проверенного и связанного региона. */
export function buildCountryVisualRegion(
  joinedRegion: JoinedCountrySnapshotRegion,
): CountryVisualRegion {
  const data = joinedRegion.data;
  const groups = buildVisualGroups(data);
  const base = {
    regionId: data.regionId,
    geometry: joinedRegion.geometry,
    data,
    groups,
  } as const;

  if (data.eventCount === 0) {
    return Object.freeze({ ...base, status: "no-events" });
  }

  if (data.coloredEventCount === 0) {
    return Object.freeze({ ...base, status: "missing-tone-only" });
  }

  return Object.freeze({
    ...base,
    status: "tone-mixture",
    pattern: buildCountryTonePattern(data, groups),
  });
}

/** Строит visual model всего принятого snapshot, не меняя factual данные. */
export function buildCountryVisualModel(
  joinedSnapshot: JoinedCountrySnapshot,
): readonly CountryVisualRegion[] {
  return Object.freeze(
    joinedSnapshot.regions.map((region) => buildCountryVisualRegion(region)),
  );
}

function buildVisualGroups(
  data: CountrySnapshotRegion,
): readonly CountryToneVisualGroup[] {
  return Object.freeze(
    COUNTRY_TONE_KEYS.map((key) => {
      const count = data.toneCounts[key];
      return Object.freeze({
        key,
        label: COUNTRY_TONE_LABELS[key],
        color: COUNTRY_TONE_PALETTE[key],
        count,
        percentage:
          data.coloredEventCount === 0
            ? 0
            : (count / data.coloredEventCount) * 100,
        visualWeight: Math.sqrt(count),
      });
    }),
  );
}

function buildCountryTonePattern(
  data: CountrySnapshotRegion,
  groups: readonly CountryToneVisualGroup[],
): CountryTonePattern {
  const configuration = COUNTRY_TONE_VISUAL_CONFIGURATION;
  const { texture, volumeScale } = configuration;
  const volume = Math.min(
    1,
    Math.sqrt(data.coloredEventCount / volumeScale.referenceColoredEventCount),
  );
  const density =
    volumeScale.densityMinimum + volumeScale.densityRange * volume;
  const opacity =
    volumeScale.opacityMinimum + volumeScale.opacityRange * volume;
  const grainCount = Math.ceil(
    (-Math.log(Math.max(texture.minimumUncoveredFraction, 1 - density)) *
      texture.width *
      texture.height) /
      (Math.PI * texture.grainRadius ** 2),
  );
  const fingerprint = JSON.stringify([
    configuration.configurationId,
    data.regionId,
    ...groups.map((group) => group.count),
  ]);
  const seed = fnv1a32(`${configuration.configurationId}|${data.regionId}`);
  const rgba = rasterizeTonePattern(groups, opacity, seed, grainCount);

  return Object.freeze({
    fingerprint,
    rgba,
    width: texture.width,
    height: texture.height,
    volume,
    density,
    opacity,
    seed,
    grainCount,
  });
}

function fnv1a32(value: string): number {
  const { fnvOffsetBasis, fnvPrime, zeroSeedReplacement } =
    COUNTRY_TONE_VISUAL_CONFIGURATION.texture;
  let hash: number = fnvOffsetBasis;

  for (let index = 0; index < value.length; index += 1) {
    hash = Math.imul(hash ^ value.charCodeAt(index), fnvPrime) >>> 0;
  }

  return hash === 0 ? zeroSeedReplacement : hash;
}

function rasterizeTonePattern(
  groups: readonly CountryToneVisualGroup[],
  opacity: number,
  seed: number,
  grainCount: number,
): Uint8Array<ArrayBuffer> {
  const { width, height, pixelExtent } =
    COUNTRY_TONE_VISUAL_CONFIGURATION.texture;
  const rgba = new Uint8Array(width * height * 4);
  const totalVisualWeight = groups.reduce(
    (total, group) => total + group.visualWeight,
    0,
  );

  if (totalVisualWeight <= 0) {
    throw new Error("Окрашенный регион не содержит положительной tone-группы");
  }

  let randomState = seed;
  for (let grainIndex = 0; grainIndex < grainCount; grainIndex += 1) {
    const centerXRandom = nextXorshift32(randomState);
    randomState = centerXRandom.state;
    const centerYRandom = nextXorshift32(randomState);
    randomState = centerYRandom.state;
    const groupRandom = nextXorshift32(randomState);
    randomState = groupRandom.state;

    const centerX = centerXRandom.value * width;
    const centerY = centerYRandom.value * height;
    const group = selectToneGroup(groups, totalVisualWeight, groupRandom.value);
    rasterizeGrain(rgba, centerX, centerY, group.key, opacity, pixelExtent);
  }

  return rgba;
}

function nextXorshift32(state: number): Readonly<{
  state: number;
  value: number;
}> {
  let nextState = (state ^ (state << 13)) >>> 0;
  nextState = (nextState ^ (nextState >>> 17)) >>> 0;
  nextState = (nextState ^ (nextState << 5)) >>> 0;
  return {
    state: nextState,
    value: nextState / 4_294_967_296,
  };
}

function selectToneGroup(
  groups: readonly CountryToneVisualGroup[],
  totalVisualWeight: number,
  randomValue: number,
): CountryToneVisualGroup {
  let cumulativeWeight = 0;
  let lastPositiveGroup: CountryToneVisualGroup | undefined;

  for (const group of groups) {
    if (group.visualWeight <= 0) {
      continue;
    }

    lastPositiveGroup = group;
    cumulativeWeight += group.visualWeight / totalVisualWeight;
    if (randomValue < cumulativeWeight) {
      return group;
    }
  }

  if (lastPositiveGroup === undefined) {
    throw new Error("Окрашенный регион не содержит tone-группы для зерна");
  }

  return lastPositiveGroup;
}

function rasterizeGrain(
  rgba: Uint8Array<ArrayBuffer>,
  centerX: number,
  centerY: number,
  toneKey: CountryToneKey,
  opacity: number,
  pixelExtent: number,
): void {
  const { width, height, grainRadius } =
    COUNTRY_TONE_VISUAL_CONFIGURATION.texture;
  const color = COUNTRY_TONE_RGB[toneKey];
  const startX = Math.floor(centerX) - pixelExtent;
  const endX = Math.floor(centerX) + pixelExtent;
  const startY = Math.floor(centerY) - pixelExtent;
  const endY = Math.floor(centerY) + pixelExtent;

  for (let y = startY; y <= endY; y += 1) {
    for (let x = startX; x <= endX; x += 1) {
      const distance = Math.hypot(x + 0.5 - centerX, y + 0.5 - centerY);
      const coverage = Math.max(0, Math.min(1, grainRadius + 0.5 - distance));
      const alpha = Math.round(opacity * coverage * 255);

      if (alpha === 0) {
        continue;
      }

      const wrappedX = ((x % width) + width) % width;
      const wrappedY = ((y % height) + height) % height;
      const byteIndex = (wrappedY * width + wrappedX) * 4;

      if (rgba[byteIndex + 3] !== 0) {
        continue;
      }

      rgba[byteIndex] = color[0];
      rgba[byteIndex + 1] = color[1];
      rgba[byteIndex + 2] = color[2];
      rgba[byteIndex + 3] = alpha;
    }
  }
}
