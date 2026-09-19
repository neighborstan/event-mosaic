import {
  COUNTRY_TONE_KEYS,
  type CountryToneKey,
  type JoinedCountrySnapshot,
  type JoinedCountrySnapshotRegion,
} from "../api/countrySnapshot";

export type CountryMapDisplayMode = "tone" | "volume";
export type CountryToneVisualGroup = Readonly<{
  key: CountryToneKey;
  label: string;
  color: string;
  count: number;
  percentage: number;
}>;

export const COUNTRY_TONE_LEGEND_ITEMS = [
  { key: "NEGATIVE_EXTREME", label: "Крайне негативный", color: "#7b1f24" },
  { key: "NEGATIVE_STRONG", label: "Сильно негативный", color: "#bd3a2b" },
  { key: "NEGATIVE_MILD", label: "Умеренно негативный", color: "#e9834b" },
  { key: "ZERO", label: "Тон ровно 0", color: "#929ba5" },
  { key: "POSITIVE_MILD", label: "Умеренно позитивный", color: "#64bec3" },
  { key: "POSITIVE_STRONG", label: "Сильно позитивный", color: "#177f99" },
  { key: "POSITIVE_EXTREME", label: "Крайне позитивный", color: "#174f78" },
] as const satisfies readonly Readonly<{
  key: CountryToneKey;
  label: string;
  color: string;
}>[];

export const COUNTRY_TONE_SUMMARIES = {
  negative: {
    label: "Негативная",
    color: "#c8554b",
    description: "80% и более отрицательных",
  },
  mostlyNegative: {
    label: "Скорее негативная",
    color: "#e6aa93",
    description: "От 60% до 80% отрицательных",
  },
  mixed: {
    label: "Смешанная",
    color: "#b2a3c9",
    description: "Нет направления с долей от 60%",
  },
  mostlyPositive: {
    label: "Скорее позитивная",
    color: "#8bc4ce",
    description: "От 60% до 80% положительных",
  },
  positive: {
    label: "Позитивная",
    color: "#39849b",
    description: "80% и более положительных",
  },
  zero: {
    label: "Преобладает тон 0",
    color: "#bfc5cf",
    description: "60% и более с тоном ровно 0",
  },
} as const;

export const COUNTRY_VOLUME_LEVELS = [
  { minimum: 1, label: "1-9", color: "#d6e9ef" },
  { minimum: 10, label: "10-99", color: "#a4cadb" },
  { minimum: 100, label: "100-999", color: "#6ca6c1" },
  { minimum: 1_000, label: "1 000-9 999", color: "#377e9e" },
  { minimum: 10_000, label: "10 000 и более", color: "#18516f" },
] as const;
export const COUNTRY_EMPTY_COLOR = "#eef1f3";
export const COUNTRY_MISSING_TONE_COLOR = "#7f8a8f";
export type CountryToneSummaryKey = keyof typeof COUNTRY_TONE_SUMMARIES;

export type CountryVisualRegion = Readonly<{
  regionId: string;
  geometry: JoinedCountrySnapshotRegion["geometry"];
  data: JoinedCountrySnapshotRegion["data"];
  groups: readonly CountryToneVisualGroup[];
  fillColor: string;
}> &
  (
    | Readonly<{ status: "no-events" }>
    | Readonly<{ status: "missing-tone-only" }>
    | Readonly<{
        status: "tone-mixture";
        summaryKey: CountryToneSummaryKey;
        negativePercentage: number;
        zeroPercentage: number;
        positivePercentage: number;
      }>
  );

/** Показывает состав тональности или объем, сохраняя исходные количества. */
export function buildCountryVisualRegion(
  joinedRegion: JoinedCountrySnapshotRegion,
  mode: CountryMapDisplayMode = "tone",
): CountryVisualRegion {
  const { data, geometry } = joinedRegion;
  const groups = COUNTRY_TONE_LEGEND_ITEMS.map((item) => ({
    ...item,
    count: data.toneCounts[item.key],
    percentage:
      data.coloredEventCount === 0
        ? 0
        : (data.toneCounts[item.key] / data.coloredEventCount) * 100,
  }));
  const base = { regionId: data.regionId, geometry, data, groups };
  let volumeColor: string = COUNTRY_EMPTY_COLOR;
  for (const level of COUNTRY_VOLUME_LEVELS) {
    if (data.eventCount >= level.minimum) volumeColor = level.color;
  }
  if (data.eventCount === 0) {
    return { ...base, status: "no-events", fillColor: COUNTRY_EMPTY_COLOR };
  }
  if (data.coloredEventCount === 0) {
    return {
      ...base,
      status: "missing-tone-only",
      fillColor: mode === "volume" ? volumeColor : COUNTRY_MISSING_TONE_COLOR,
    };
  }
  const negative =
    COUNTRY_TONE_KEYS.slice(0, 3).reduce(
      (sum, key) => sum + data.toneCounts[key],
      0,
    ) / data.coloredEventCount;
  const zero = data.toneCounts.ZERO / data.coloredEventCount;
  const positive =
    COUNTRY_TONE_KEYS.slice(4).reduce(
      (sum, key) => sum + data.toneCounts[key],
      0,
    ) / data.coloredEventCount;
  const summaryKey = summarizeTone(negative, zero, positive);
  return {
    ...base,
    status: "tone-mixture",
    summaryKey,
    negativePercentage: negative * 100,
    zeroPercentage: zero * 100,
    positivePercentage: positive * 100,
    fillColor:
      mode === "volume"
        ? volumeColor
        : COUNTRY_TONE_SUMMARIES[summaryKey].color,
  };
}

/** Строит сводную окраску стран без текстур и вымышленных мест событий. */
export function buildCountryVisualModel(
  joinedSnapshot: JoinedCountrySnapshot,
  mode: CountryMapDisplayMode = "tone",
): readonly CountryVisualRegion[] {
  return joinedSnapshot.regions.map((region) =>
    buildCountryVisualRegion(region, mode),
  );
}

function summarizeTone(
  negative: number,
  zero: number,
  positive: number,
): CountryToneSummaryKey {
  if (negative >= 0.8) return "negative";
  if (negative >= 0.6) return "mostlyNegative";
  if (positive >= 0.8) return "positive";
  if (positive >= 0.6) return "mostlyPositive";
  if (zero >= 0.6) return "zero";
  return "mixed";
}
