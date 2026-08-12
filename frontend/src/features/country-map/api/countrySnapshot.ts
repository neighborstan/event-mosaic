import type {
  CountryGeometry,
  CountryGeometryFeature,
} from "./countryGeometry";

export const COUNTRY_SNAPSHOT_URL = "/api/v1/map/country-snapshot";

const COUNTRY_SNAPSHOT_MEDIA_TYPE = "application/json";
const COUNTRY_GEOMETRY_VERSION = "country-v1";
const COUNTRY_TONE_MODEL_VERSION = "tone-bands-v1";
const COUNTRY_REGION_COUNT = 258;
const SNAPSHOT_DURATION_MILLISECONDS = 24 * 60 * 60 * 1_000;
const SOURCE_CADENCE_MILLISECONDS = 15 * 60 * 1_000;
const COUNTRY_REGION_ID_PATTERN = /^country:[a-z0-9][a-z0-9-]{0,63}$/u;
const UTC_INSTANT_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/u;

export const COUNTRY_TONE_KEYS = [
  "NEGATIVE_EXTREME",
  "NEGATIVE_STRONG",
  "NEGATIVE_MILD",
  "ZERO",
  "POSITIVE_MILD",
  "POSITIVE_STRONG",
  "POSITIVE_EXTREME",
] as const;

const UNLOCATED_REASONS = [
  "ACTION_GEO_MISSING_OR_INVALID",
  "ACTOR_FALLBACK",
  "OTHER",
] as const;

const UNMAPPED_REASONS = [
  "COUNTRY_CODE_MISSING",
  "UNKNOWN_COUNTRY_CODE",
  "NO_REGION_GEOMETRY",
  "AMBIGUOUS_REGION_MAPPING",
  "UNSUPPORTED_COUNTRY_CODE",
  "OTHER",
] as const;

export type CountryToneKey = (typeof COUNTRY_TONE_KEYS)[number];
export type CountryCoverageStatus = "COMPLETE" | "PARTIAL" | "UNKNOWN";

export type CountryToneCounts = Readonly<{
  NEGATIVE_EXTREME: number;
  NEGATIVE_STRONG: number;
  NEGATIVE_MILD: number;
  ZERO: number;
  POSITIVE_MILD: number;
  POSITIVE_STRONG: number;
  POSITIVE_EXTREME: number;
}>;

export type CountrySnapshotRegion = Readonly<{
  regionId: string;
  eventCount: number;
  coloredEventCount: number;
  missingToneEventCount: number;
  toneCounts: CountryToneCounts;
}>;

export type CountrySnapshot = Readonly<{
  snapshot: Readonly<{
    from: string;
    to: string;
    geometryVersion: "country-v1";
    toneModelVersion: "tone-bands-v1";
  }>;
  coverage: Readonly<{
    status: CountryCoverageStatus;
    missingIntervals: readonly Readonly<{ from: string; to: string }>[] | null;
  }>;
  quality: Readonly<{
    eligibleEventCount: number;
    mappedEventCount: number;
    unlocatedEventCount: number;
    unmappedEventCount: number;
    unlocatedReasonCounts: readonly CountryReasonCount[];
    unmappedReasonCounts: readonly CountryReasonCount[];
  }>;
  regions: readonly CountrySnapshotRegion[];
}>;

export type JoinedCountrySnapshotRegion = Readonly<{
  geometry: CountryGeometryFeature;
  data: CountrySnapshotRegion;
}>;

export type JoinedCountrySnapshot = Readonly<{
  snapshot: CountrySnapshot;
  regions: readonly JoinedCountrySnapshotRegion[];
}>;

type CountryReasonCount = Readonly<{
  reason: string;
  eventCount: number;
}>;

export type CountrySnapshotFetcher = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export type CountrySnapshotLoadErrorCode =
  | "country-snapshot-http-error"
  | "country-snapshot-media-type-error"
  | "country-snapshot-json-error"
  | "country-snapshot-network-error"
  | "country-snapshot-validation-error";

export class CountrySnapshotLoadError extends Error {
  override readonly name = "CountrySnapshotLoadError";
  readonly code: CountrySnapshotLoadErrorCode;

  constructor(code: CountrySnapshotLoadErrorCode) {
    super("Не удалось безопасно загрузить данные событий");
    this.code = code;
  }
}

export class CountrySnapshotValidationError extends Error {
  override readonly name = "CountrySnapshotValidationError";
  readonly path: string;

  constructor(path: string) {
    super(`Снимок карты нарушает ожидаемый контракт: ${path}`);
    this.path = path;
  }
}

export class CountrySnapshotJoinError extends Error {
  override readonly name = "CountrySnapshotJoinError";

  constructor() {
    super("Геометрия и снимок карты не совпадают полностью");
  }
}

type LoadCountrySnapshotOptions = Readonly<{
  signal: AbortSignal;
  fetcher?: CountrySnapshotFetcher | undefined;
}>;

/** Загружает один snapshot и не выпускает непроверенный JSON за HTTP-границу. */
export async function loadCountrySnapshot({
  signal,
  fetcher = fetch,
}: LoadCountrySnapshotOptions): Promise<CountrySnapshot> {
  let response: Response;

  try {
    response = await fetcher(COUNTRY_SNAPSHOT_URL, {
      method: "GET",
      headers: { Accept: COUNTRY_SNAPSHOT_MEDIA_TYPE },
      cache: "no-store",
      signal,
    });
  } catch (error: unknown) {
    rethrowAbort(error, signal);
    throw new CountrySnapshotLoadError("country-snapshot-network-error");
  }

  if (!response.ok) {
    throw new CountrySnapshotLoadError("country-snapshot-http-error");
  }

  if (
    readPrimaryMediaType(response.headers.get("content-type")) !==
    COUNTRY_SNAPSHOT_MEDIA_TYPE
  ) {
    throw new CountrySnapshotLoadError("country-snapshot-media-type-error");
  }

  let document: unknown;
  try {
    document = await response.json();
  } catch (error: unknown) {
    rethrowAbort(error, signal);
    throw new CountrySnapshotLoadError("country-snapshot-json-error");
  }

  try {
    return validateCountrySnapshot(document);
  } catch (error: unknown) {
    if (error instanceof CountrySnapshotValidationError) {
      throw new CountrySnapshotLoadError("country-snapshot-validation-error");
    }
    throw error;
  }
}

/** Проверяет весь wire document и возвращает неизменяемую внутреннюю модель. */
export function validateCountrySnapshot(document: unknown): CountrySnapshot {
  const root = requireExactRecord(document, "$", [
    "snapshot",
    "coverage",
    "quality",
    "regions",
  ]);
  const snapshot = validateSnapshotMetadata(root["snapshot"]);
  const coverage = validateCoverage(
    root["coverage"],
    snapshot.from,
    snapshot.to,
  );
  const quality = validateQuality(root["quality"]);
  const regions = validateRegions(root["regions"]);

  if (
    sumCounts(
      regions.map((region) => region.eventCount),
      "$.regions",
    ) !== quality.mappedEventCount
  ) {
    fail("$.quality.mappedEventCount");
  }

  return Object.freeze({
    snapshot,
    coverage,
    quality,
    regions,
  });
}

/** Доказывает взаимно однозначный join exact snapshot и принятой geometry. */
export function joinCountrySnapshot(
  snapshot: CountrySnapshot,
  geometry: CountryGeometry,
): JoinedCountrySnapshot {
  if (snapshot.snapshot.geometryVersion !== geometry.geometryVersion) {
    throw new CountrySnapshotJoinError();
  }

  const regionsById = new Map(
    snapshot.regions.map((region) => [region.regionId, region]),
  );
  if (
    regionsById.size !== geometry.features.length ||
    geometry.features.length !== COUNTRY_REGION_COUNT
  ) {
    throw new CountrySnapshotJoinError();
  }

  const joinedRegions = geometry.features.map((feature) => {
    const data = regionsById.get(feature.properties.regionId);
    if (data === undefined) {
      throw new CountrySnapshotJoinError();
    }
    return Object.freeze({ geometry: feature, data });
  });

  return Object.freeze({
    snapshot,
    regions: Object.freeze(joinedRegions),
  });
}

export function isCountrySnapshotAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function validateSnapshotMetadata(
  document: unknown,
): CountrySnapshot["snapshot"] {
  const path = "$.snapshot";
  const value = requireExactRecord(document, path, [
    "from",
    "to",
    "geometryVersion",
    "toneModelVersion",
  ]);
  const from = requireCadenceInstant(value["from"], `${path}.from`);
  const to = requireCadenceInstant(value["to"], `${path}.to`);

  if (Date.parse(to) - Date.parse(from) !== SNAPSHOT_DURATION_MILLISECONDS) {
    fail(path);
  }
  requireExactValue(
    value["geometryVersion"],
    COUNTRY_GEOMETRY_VERSION,
    `${path}.geometryVersion`,
  );
  requireExactValue(
    value["toneModelVersion"],
    COUNTRY_TONE_MODEL_VERSION,
    `${path}.toneModelVersion`,
  );

  return Object.freeze({
    from,
    to,
    geometryVersion: COUNTRY_GEOMETRY_VERSION,
    toneModelVersion: COUNTRY_TONE_MODEL_VERSION,
  });
}

function validateCoverage(
  document: unknown,
  snapshotFrom: string,
  snapshotTo: string,
): CountrySnapshot["coverage"] {
  const path = "$.coverage";
  const value = requireExactRecord(document, path, [
    "status",
    "missingIntervals",
  ]);
  const status = value["status"];
  if (status !== "COMPLETE" && status !== "PARTIAL" && status !== "UNKNOWN") {
    fail(`${path}.status`);
  }

  const intervalDocuments = value["missingIntervals"];
  if (status === "UNKNOWN") {
    if (intervalDocuments !== null) {
      fail(`${path}.missingIntervals`);
    }
    return Object.freeze({ status, missingIntervals: null });
  }
  if (!Array.isArray(intervalDocuments)) {
    fail(`${path}.missingIntervals`);
  }
  if (
    (status === "COMPLETE" && intervalDocuments.length !== 0) ||
    (status === "PARTIAL" && intervalDocuments.length === 0)
  ) {
    fail(`${path}.missingIntervals`);
  }

  const snapshotFromMillis = Date.parse(snapshotFrom);
  const snapshotToMillis = Date.parse(snapshotTo);
  let previousTo = Number.NEGATIVE_INFINITY;
  const missingIntervals = intervalDocuments.map((interval, index) => {
    const intervalPath = `${path}.missingIntervals[${index}]`;
    const value = requireExactRecord(interval, intervalPath, ["from", "to"]);
    const from = requireCadenceInstant(value["from"], `${intervalPath}.from`);
    const to = requireCadenceInstant(value["to"], `${intervalPath}.to`);
    const fromMillis = Date.parse(from);
    const toMillis = Date.parse(to);
    if (
      fromMillis < snapshotFromMillis ||
      toMillis > snapshotToMillis ||
      fromMillis >= toMillis ||
      fromMillis <= previousTo
    ) {
      fail(intervalPath);
    }
    previousTo = toMillis;
    return Object.freeze({ from, to });
  });

  return Object.freeze({
    status,
    missingIntervals: Object.freeze(missingIntervals),
  });
}

function validateQuality(document: unknown): CountrySnapshot["quality"] {
  const path = "$.quality";
  const value = requireExactRecord(document, path, [
    "eligibleEventCount",
    "mappedEventCount",
    "unlocatedEventCount",
    "unmappedEventCount",
    "unlocatedReasonCounts",
    "unmappedReasonCounts",
  ]);
  const eligibleEventCount = requireSafeCount(
    value["eligibleEventCount"],
    `${path}.eligibleEventCount`,
  );
  const mappedEventCount = requireSafeCount(
    value["mappedEventCount"],
    `${path}.mappedEventCount`,
  );
  const unlocatedEventCount = requireSafeCount(
    value["unlocatedEventCount"],
    `${path}.unlocatedEventCount`,
  );
  const unmappedEventCount = requireSafeCount(
    value["unmappedEventCount"],
    `${path}.unmappedEventCount`,
  );
  const unlocatedReasonCounts = validateReasonCounts(
    value["unlocatedReasonCounts"],
    `${path}.unlocatedReasonCounts`,
    UNLOCATED_REASONS,
  );
  const unmappedReasonCounts = validateReasonCounts(
    value["unmappedReasonCounts"],
    `${path}.unmappedReasonCounts`,
    UNMAPPED_REASONS,
  );

  if (
    sumCounts(
      [mappedEventCount, unlocatedEventCount, unmappedEventCount],
      path,
    ) !== eligibleEventCount ||
    sumCounts(
      unlocatedReasonCounts.map((reason) => reason.eventCount),
      `${path}.unlocatedReasonCounts`,
    ) !== unlocatedEventCount ||
    sumCounts(
      unmappedReasonCounts.map((reason) => reason.eventCount),
      `${path}.unmappedReasonCounts`,
    ) !== unmappedEventCount
  ) {
    fail(path);
  }

  return Object.freeze({
    eligibleEventCount,
    mappedEventCount,
    unlocatedEventCount,
    unmappedEventCount,
    unlocatedReasonCounts,
    unmappedReasonCounts,
  });
}

function validateReasonCounts(
  document: unknown,
  path: string,
  reasons: readonly string[],
): readonly CountryReasonCount[] {
  if (!Array.isArray(document) || document.length !== reasons.length) {
    fail(path);
  }

  return Object.freeze(
    document.map((reasonDocument, index) => {
      const reasonPath = `${path}[${index}]`;
      const value = requireExactRecord(reasonDocument, reasonPath, [
        "reason",
        "eventCount",
      ]);
      requireExactValue(
        value["reason"],
        reasons[index],
        `${reasonPath}.reason`,
      );
      return Object.freeze({
        reason: reasons[index] ?? fail(`${reasonPath}.reason`),
        eventCount: requireSafeCount(
          value["eventCount"],
          `${reasonPath}.eventCount`,
        ),
      });
    }),
  );
}

function validateRegions(document: unknown): readonly CountrySnapshotRegion[] {
  const path = "$.regions";
  if (!Array.isArray(document) || document.length !== COUNTRY_REGION_COUNT) {
    fail(path);
  }

  let previousRegionId: string | null = null;
  const regions = document.map((regionDocument, index) => {
    const regionPath = `${path}[${index}]`;
    const value = requireExactRecord(regionDocument, regionPath, [
      "regionId",
      "eventCount",
      "coloredEventCount",
      "missingToneEventCount",
      "toneCounts",
    ]);
    const regionId = value["regionId"];
    if (
      typeof regionId !== "string" ||
      !COUNTRY_REGION_ID_PATTERN.test(regionId) ||
      (previousRegionId !== null && previousRegionId >= regionId)
    ) {
      fail(`${regionPath}.regionId`);
    }
    previousRegionId = regionId;
    const eventCount = requireSafeCount(
      value["eventCount"],
      `${regionPath}.eventCount`,
    );
    const coloredEventCount = requireSafeCount(
      value["coloredEventCount"],
      `${regionPath}.coloredEventCount`,
    );
    const missingToneEventCount = requireSafeCount(
      value["missingToneEventCount"],
      `${regionPath}.missingToneEventCount`,
    );
    const toneCounts = validateToneCounts(
      value["toneCounts"],
      `${regionPath}.toneCounts`,
    );
    if (
      sumCounts(
        COUNTRY_TONE_KEYS.map((key) => toneCounts[key]),
        `${regionPath}.toneCounts`,
      ) !== coloredEventCount ||
      sumCounts([coloredEventCount, missingToneEventCount], regionPath) !==
        eventCount
    ) {
      fail(regionPath);
    }

    return Object.freeze({
      regionId,
      eventCount,
      coloredEventCount,
      missingToneEventCount,
      toneCounts,
    });
  });

  return Object.freeze(regions);
}

function validateToneCounts(
  document: unknown,
  path: string,
): CountryToneCounts {
  const value = requireExactRecord(document, path, COUNTRY_TONE_KEYS);
  return Object.freeze({
    NEGATIVE_EXTREME: requireSafeCount(
      value["NEGATIVE_EXTREME"],
      `${path}.NEGATIVE_EXTREME`,
    ),
    NEGATIVE_STRONG: requireSafeCount(
      value["NEGATIVE_STRONG"],
      `${path}.NEGATIVE_STRONG`,
    ),
    NEGATIVE_MILD: requireSafeCount(
      value["NEGATIVE_MILD"],
      `${path}.NEGATIVE_MILD`,
    ),
    ZERO: requireSafeCount(value["ZERO"], `${path}.ZERO`),
    POSITIVE_MILD: requireSafeCount(
      value["POSITIVE_MILD"],
      `${path}.POSITIVE_MILD`,
    ),
    POSITIVE_STRONG: requireSafeCount(
      value["POSITIVE_STRONG"],
      `${path}.POSITIVE_STRONG`,
    ),
    POSITIVE_EXTREME: requireSafeCount(
      value["POSITIVE_EXTREME"],
      `${path}.POSITIVE_EXTREME`,
    ),
  });
}

function requireCadenceInstant(document: unknown, path: string): string {
  if (typeof document !== "string" || !UTC_INSTANT_PATTERN.test(document)) {
    fail(path);
  }
  const milliseconds = Date.parse(document);
  if (
    !Number.isFinite(milliseconds) ||
    new Date(milliseconds).toISOString().replace(".000Z", "Z") !== document ||
    ((milliseconds % SOURCE_CADENCE_MILLISECONDS) +
      SOURCE_CADENCE_MILLISECONDS) %
      SOURCE_CADENCE_MILLISECONDS !==
      0
  ) {
    fail(path);
  }
  return document;
}

function requireSafeCount(document: unknown, path: string): number {
  if (
    typeof document !== "number" ||
    !Number.isSafeInteger(document) ||
    document < 0
  ) {
    fail(path);
  }
  return document;
}

function sumCounts(counts: readonly number[], path: string): number {
  let total = 0;
  for (const count of counts) {
    total += count;
    if (!Number.isSafeInteger(total)) {
      fail(path);
    }
  }
  return total;
}

function requireExactRecord(
  document: unknown,
  path: string,
  expectedKeys: readonly string[],
): Record<string, unknown> {
  if (!isRecord(document)) {
    fail(path);
  }
  const actualKeys = Object.keys(document);
  if (
    actualKeys.length !== expectedKeys.length ||
    expectedKeys.some((key) => !Object.hasOwn(document, key))
  ) {
    fail(path);
  }
  return document;
}

function isRecord(document: unknown): document is Record<string, unknown> {
  return (
    typeof document === "object" &&
    document !== null &&
    !Array.isArray(document)
  );
}

function requireExactValue(
  actual: unknown,
  expected: string | undefined,
  path: string,
): void {
  if (expected === undefined || actual !== expected) {
    fail(path);
  }
}

function readPrimaryMediaType(contentType: string | null): string | null {
  const [primaryMediaType] = contentType?.split(";", 1) ?? [];
  const normalized = primaryMediaType?.trim().toLowerCase();
  return normalized === undefined || normalized.length === 0
    ? null
    : normalized;
}

function rethrowAbort(error: unknown, signal: AbortSignal): void {
  if (isCountrySnapshotAbortError(error)) {
    throw error;
  }
  if (signal.aborted) {
    throw new DOMException("Загрузка снимка отменена", "AbortError");
  }
}

function fail(path: string): never {
  throw new CountrySnapshotValidationError(path);
}
