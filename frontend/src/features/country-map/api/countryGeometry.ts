export const COUNTRY_GEOMETRY_URL =
  "/map/geometry/country-v1/countries.geojson";

const COUNTRY_GEOMETRY_VERSION = "country-v1";
const COUNTRY_FEATURE_COUNT = 258;
const COUNTRY_REGION_ID_PATTERN = /^country:[a-z0-9][a-z0-9-]{0,63}$/u;
const COUNTRY_GEOMETRY_MEDIA_TYPE = "application/geo+json";
const COUNTRY_GEOMETRY_SOURCE = "natural-earth-10m";

export type CountryDisputeStatus = "STANDARD" | "DISPUTED_DE_FACTO";
export type CountryPosition = readonly [number, number];
export type CountryLinearRing = readonly CountryPosition[];
export type CountryPolygonCoordinates = readonly CountryLinearRing[];
export type CountryMultiPolygonCoordinates =
  readonly CountryPolygonCoordinates[];

export type CountryGeometryShape =
  | Readonly<{
      type: "Polygon";
      coordinates: CountryPolygonCoordinates;
    }>
  | Readonly<{
      type: "MultiPolygon";
      coordinates: CountryMultiPolygonCoordinates;
    }>;

export type CountryGeometryProperties = Readonly<{
  regionId: string;
  displayName: string;
  disputeStatus: CountryDisputeStatus;
  geometrySource: "natural-earth-10m";
}>;

export type CountryGeometryFeature = Readonly<{
  type: "Feature";
  properties: CountryGeometryProperties;
  geometry: CountryGeometryShape;
}>;

export type CountryGeometry = Readonly<{
  type: "FeatureCollection";
  geometryVersion: "country-v1";
  features: readonly CountryGeometryFeature[];
}>;

export type CountryGeometryFetcher = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export type CountryGeometryLoadErrorCode =
  | "country-geometry-http-error"
  | "country-geometry-media-type-error"
  | "country-geometry-json-error"
  | "country-geometry-network-error"
  | "country-geometry-validation-error";

export class CountryGeometryLoadError extends Error {
  override readonly name = "CountryGeometryLoadError";
  readonly code: CountryGeometryLoadErrorCode;

  constructor(code: CountryGeometryLoadErrorCode) {
    super("Не удалось безопасно загрузить геометрию стран");
    this.code = code;
  }
}

export class CountryGeometryValidationError extends Error {
  override readonly name = "CountryGeometryValidationError";
  readonly path: string;

  constructor(path: string) {
    super(`Геометрия стран нарушает ожидаемый контракт: ${path}`);
    this.path = path;
  }
}

type LoadCountryGeometryOptions = Readonly<{
  signal: AbortSignal;
  fetcher?: CountryGeometryFetcher | undefined;
}>;

/**
 * Загружает только опубликованную country-v1 и не передает непроверенный JSON
 * дальше границы HTTP.
 */
export async function loadCountryGeometry({
  signal,
  fetcher = fetch,
}: LoadCountryGeometryOptions): Promise<CountryGeometry> {
  let response: Response;

  try {
    response = await fetcher(COUNTRY_GEOMETRY_URL, {
      method: "GET",
      headers: {
        Accept: COUNTRY_GEOMETRY_MEDIA_TYPE,
      },
      signal,
    });
  } catch (error: unknown) {
    rethrowAbort(error, signal);
    throw new CountryGeometryLoadError("country-geometry-network-error");
  }

  if (!response.ok) {
    throw new CountryGeometryLoadError("country-geometry-http-error");
  }

  if (
    readPrimaryMediaType(response.headers.get("content-type")) !==
    COUNTRY_GEOMETRY_MEDIA_TYPE
  ) {
    throw new CountryGeometryLoadError("country-geometry-media-type-error");
  }

  let document: unknown;

  try {
    document = await response.json();
  } catch (error: unknown) {
    rethrowAbort(error, signal);
    throw new CountryGeometryLoadError("country-geometry-json-error");
  }

  try {
    return validateCountryGeometry(document);
  } catch (error: unknown) {
    if (error instanceof CountryGeometryValidationError) {
      throw new CountryGeometryLoadError("country-geometry-validation-error");
    }

    throw error;
  }
}

/**
 * Проверяет закрытый browser contract country-v1 и возвращает новую глубоко
 * неизменяемую модель только после успешной проверки всего документа.
 */
export function validateCountryGeometry(document: unknown): CountryGeometry {
  const root = requireExactRecord(document, "$", [
    "type",
    "geometryVersion",
    "features",
  ]);

  requireExactValue(root["type"], "FeatureCollection", "$.type");
  requireExactValue(
    root["geometryVersion"],
    COUNTRY_GEOMETRY_VERSION,
    "$.geometryVersion",
  );

  const featureDocuments = root["features"];

  if (
    !isUnknownArray(featureDocuments) ||
    featureDocuments.length !== COUNTRY_FEATURE_COUNT
  ) {
    fail("$.features");
  }

  const regionIds = new Set<string>();
  const features = featureDocuments.map((feature, index) =>
    validateFeature(feature, index, regionIds),
  );

  return Object.freeze({
    type: "FeatureCollection",
    geometryVersion: COUNTRY_GEOMETRY_VERSION,
    features: Object.freeze(features),
  });
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function validateFeature(
  document: unknown,
  index: number,
  regionIds: Set<string>,
): CountryGeometryFeature {
  const path = `$.features[${index}]`;
  const feature = requireExactRecord(document, path, [
    "type",
    "properties",
    "geometry",
  ]);

  requireExactValue(feature["type"], "Feature", `${path}.type`);
  const properties = validateProperties(feature["properties"], path);

  if (regionIds.has(properties.regionId)) {
    fail(`${path}.properties.regionId`);
  }

  regionIds.add(properties.regionId);

  return Object.freeze({
    type: "Feature",
    properties,
    geometry: validateGeometry(feature["geometry"], path),
  });
}

function validateProperties(
  document: unknown,
  featurePath: string,
): CountryGeometryProperties {
  const path = `${featurePath}.properties`;
  const properties = requireExactRecord(document, path, [
    "regionId",
    "displayName",
    "disputeStatus",
    "geometrySource",
  ]);
  const regionId = properties["regionId"];
  const displayName = properties["displayName"];
  const disputeStatus = properties["disputeStatus"];

  if (
    typeof regionId !== "string" ||
    !COUNTRY_REGION_ID_PATTERN.test(regionId)
  ) {
    fail(`${path}.regionId`);
  }

  if (typeof displayName !== "string" || displayName.trim().length === 0) {
    fail(`${path}.displayName`);
  }

  if (disputeStatus !== "STANDARD" && disputeStatus !== "DISPUTED_DE_FACTO") {
    fail(`${path}.disputeStatus`);
  }

  requireExactValue(
    properties["geometrySource"],
    COUNTRY_GEOMETRY_SOURCE,
    `${path}.geometrySource`,
  );

  return Object.freeze({
    regionId,
    displayName,
    disputeStatus,
    geometrySource: COUNTRY_GEOMETRY_SOURCE,
  });
}

function validateGeometry(
  document: unknown,
  featurePath: string,
): CountryGeometryShape {
  const path = `${featurePath}.geometry`;
  const geometry = requireExactRecord(document, path, ["type", "coordinates"]);

  switch (geometry["type"]) {
    case "Polygon":
      return Object.freeze({
        type: "Polygon",
        coordinates: validatePolygonCoordinates(
          geometry["coordinates"],
          `${path}.coordinates`,
        ),
      });
    case "MultiPolygon":
      return Object.freeze({
        type: "MultiPolygon",
        coordinates: validateMultiPolygonCoordinates(
          geometry["coordinates"],
          `${path}.coordinates`,
        ),
      });
    default:
      return fail(`${path}.type`);
  }
}

function validateMultiPolygonCoordinates(
  document: unknown,
  path: string,
): CountryMultiPolygonCoordinates {
  if (!isUnknownArray(document) || document.length === 0) {
    fail(path);
  }

  return Object.freeze(
    document.map((polygon, index) =>
      validatePolygonCoordinates(polygon, `${path}[${index}]`),
    ),
  );
}

function validatePolygonCoordinates(
  document: unknown,
  path: string,
): CountryPolygonCoordinates {
  if (!isUnknownArray(document) || document.length === 0) {
    fail(path);
  }

  return Object.freeze(
    document.map((ring, index) =>
      validateLinearRing(ring, `${path}[${index}]`),
    ),
  );
}

function validateLinearRing(
  document: unknown,
  path: string,
): CountryLinearRing {
  if (!isUnknownArray(document) || document.length < 4) {
    fail(path);
  }

  const positions = document.map((position, index) =>
    validatePosition(position, `${path}[${index}]`),
  );
  const first = positions[0];
  const last = positions.at(-1);

  if (first === undefined || first[0] !== last?.[0] || first[1] !== last?.[1]) {
    fail(path);
  }

  return Object.freeze(positions);
}

function validatePosition(document: unknown, path: string): CountryPosition {
  if (!isUnknownArray(document) || document.length !== 2) {
    fail(path);
  }

  const longitude = document[0];
  const latitude = document[1];

  if (
    typeof longitude !== "number" ||
    !Number.isFinite(longitude) ||
    longitude < -180 ||
    longitude > 180
  ) {
    fail(`${path}[0]`);
  }

  if (
    typeof latitude !== "number" ||
    !Number.isFinite(latitude) ||
    latitude < -90 ||
    latitude > 90
  ) {
    fail(`${path}[1]`);
  }

  return Object.freeze([longitude, latitude] as const);
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

function isUnknownArray(document: unknown): document is unknown[] {
  return Array.isArray(document);
}

function requireExactValue(
  actual: unknown,
  expected: string,
  path: string,
): void {
  if (actual !== expected) {
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
  if (isAbortError(error)) {
    throw error;
  }

  if (signal.aborted) {
    throw new DOMException("Загрузка геометрии отменена", "AbortError");
  }
}

function fail(path: string): never {
  throw new CountryGeometryValidationError(path);
}
