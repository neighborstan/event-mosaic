import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { validateCountryCrosswalkStructure } from "./country-crosswalk.mjs";
import { sha256Hex } from "./canonical-json.mjs";
import { validateSourceManifest } from "./source-manifest.mjs";

const GEOMETRY_VERSION_PATTERN = /^country-v[1-9][0-9]*$/;
const PROFILE_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;
const TOOL_VERSION_PATTERN = /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/;

const CONFIG_FIELDS = [
  "geometryConfigSchemaVersion",
  "geometryVersion",
  "geometrySource",
  "toolchain",
  "candidateProfiles",
  "selectedProfileId",
  "gzip",
  "budgets",
];
const TOOLCHAIN_FIELDS = [
  "toolVersion",
  "nodeVersion",
  "npmVersion",
  "mapshaperVersion",
];
const PROFILE_FIELDS = [
  "profileId",
  "simplificationMethod",
  "retainPercent",
  "keepShapes",
  "coordinatePrecision",
];
const GZIP_FIELDS = ["level", "mtime"];
const BUDGET_FIELDS = [
  "maxGeojsonBytes",
  "maxGzipBytes",
  "maxVertices",
];

export async function readGeometryProjectContext(toolRoot) {
  const paths = {
    geometryConfig: join(toolRoot, "data", "geometry-config.json"),
    sourceManifest: join(toolRoot, "data", "source-manifest.json"),
    crosswalk: join(toolRoot, "data", "country-crosswalk.json"),
    packageJson: join(toolRoot, "package.json"),
    packageLock: join(toolRoot, "package-lock.json"),
  };
  const [
    geometryConfigBytes,
    sourceManifestBytes,
    crosswalkBytes,
    packageJsonBytes,
    packageLockBytes,
  ] = await Promise.all(
    Object.values(paths).map((path) => readFile(path)),
  );

  const geometryConfig = parseJson(geometryConfigBytes, "geometry config");
  const sourceManifest = parseJson(sourceManifestBytes, "source manifest");
  const crosswalk = parseJson(crosswalkBytes, "country crosswalk");
  const packageJson = parseJson(packageJsonBytes, "tool package.json");
  const packageLock = parseJson(packageLockBytes, "tool package-lock.json");

  validateGeometryConfig(geometryConfig);
  validateSourceManifest(sourceManifest);
  const crosswalkValidation = validateCountryCrosswalkStructure(crosswalk);
  validateToolMetadata({ geometryConfig, packageJson, packageLock });

  return createGeometryProjectContext({
    toolRoot,
    paths,
    geometryConfig,
    geometryConfigBytes,
    sourceManifest,
    sourceManifestBytes,
    crosswalk,
    crosswalkBytes,
    crosswalkValidation,
    packageJson,
    packageJsonBytes,
    packageLock,
    packageLockBytes,
  });
}

export function createGeometryProjectContext({
  toolRoot = null,
  paths = null,
  geometryConfig,
  geometryConfigBytes,
  sourceManifest,
  sourceManifestBytes,
  crosswalk,
  crosswalkBytes,
  crosswalkValidation = validateCountryCrosswalkStructure(crosswalk),
  packageJson,
  packageJsonBytes,
  packageLock,
  packageLockBytes,
}) {
  validateGeometryConfig(geometryConfig);
  validateSourceManifest(sourceManifest);
  validateToolMetadata({ geometryConfig, packageJson, packageLock });
  validateCrosswalkSourceReferences(crosswalk, sourceManifest);

  const context = {
    toolRoot,
    paths,
    geometryConfig,
    sourceManifest,
    crosswalk,
    crosswalkValidation,
    packageJson,
    packageLock,
    hashes: {
      geometryConfigSha256: sha256Hex(geometryConfigBytes),
      sourceManifestSha256: sha256Hex(sourceManifestBytes),
      crosswalkSha256: sha256Hex(crosswalkBytes),
      packageJsonSha256: sha256Hex(packageJsonBytes),
      packageLockSha256: sha256Hex(packageLockBytes),
    },
  };
  context.regions = buildRuntimeRegionCatalog(crosswalk);
  context.unmappedGdeltCountryCodes = buildUnmappedCatalog(crosswalk);
  return context;
}

export function validateGeometryConfig(config) {
  assertStrictObject(config, "geometryConfig", CONFIG_FIELDS);
  assertExact(config.geometryConfigSchemaVersion, 1, "geometryConfig.geometryConfigSchemaVersion");
  assertPattern(
    config.geometryVersion,
    GEOMETRY_VERSION_PATTERN,
    "geometryConfig.geometryVersion",
  );
  assertExact(
    config.geometrySource,
    "natural-earth-10m",
    "geometryConfig.geometrySource",
  );

  assertStrictObject(config.toolchain, "geometryConfig.toolchain", TOOLCHAIN_FIELDS);
  for (const field of TOOLCHAIN_FIELDS) {
    assertPattern(
      config.toolchain[field],
      TOOL_VERSION_PATTERN,
      `geometryConfig.toolchain.${field}`,
    );
  }

  if (!Array.isArray(config.candidateProfiles) || config.candidateProfiles.length < 3) {
    fail("geometryConfig.candidateProfiles", "ожидаются минимум три candidate profile");
  }
  const profileIds = new Set();
  for (const [index, profile] of config.candidateProfiles.entries()) {
    const path = `geometryConfig.candidateProfiles[${index}]`;
    assertStrictObject(profile, path, PROFILE_FIELDS);
    assertPattern(profile.profileId, PROFILE_ID_PATTERN, `${path}.profileId`);
    if (profileIds.has(profile.profileId)) {
      fail(`${path}.profileId`, `candidate profile повторяется: ${profile.profileId}`);
    }
    profileIds.add(profile.profileId);
    assertExact(profile.simplificationMethod, "weighted", `${path}.simplificationMethod`);
    assertFiniteNumberInRange(profile.retainPercent, 0, 100, `${path}.retainPercent`, false);
    assertExact(profile.keepShapes, true, `${path}.keepShapes`);
    assertFiniteNumberInRange(
      profile.coordinatePrecision,
      0,
      1,
      `${path}.coordinatePrecision`,
      false,
    );
    assertCoordinatePrecision(profile.coordinatePrecision, `${path}.coordinatePrecision`);
  }
  assertPattern(config.selectedProfileId, PROFILE_ID_PATTERN, "geometryConfig.selectedProfileId");
  if (!profileIds.has(config.selectedProfileId)) {
    fail(
      "geometryConfig.selectedProfileId",
      `неизвестный candidate profile: ${config.selectedProfileId}`,
    );
  }

  assertStrictObject(config.gzip, "geometryConfig.gzip", GZIP_FIELDS);
  assertSafeIntegerInRange(config.gzip.level, 0, 9, "geometryConfig.gzip.level");
  assertExact(config.gzip.mtime, 0, "geometryConfig.gzip.mtime");

  assertStrictObject(config.budgets, "geometryConfig.budgets", BUDGET_FIELDS);
  for (const field of BUDGET_FIELDS) {
    assertPositiveSafeInteger(config.budgets[field], `geometryConfig.budgets.${field}`);
  }
  return config;
}

export function selectCandidateProfile(config, profileId = config.selectedProfileId) {
  const profile = config.candidateProfiles.find(
    (candidate) => candidate.profileId === profileId,
  );
  if (profile === undefined) {
    throw new TypeError(`Неизвестный candidate profile: ${String(profileId)}`);
  }
  return profile;
}

export function assertSupportedRuntime(config) {
  if (process.versions.node !== config.toolchain.nodeVersion) {
    throw new Error(
      `Подготовка требует Node.js ${config.toolchain.nodeVersion}, запущен ${process.versions.node}`,
    );
  }
}

export function buildRuntimeRegionCatalog(crosswalk) {
  return [...crosswalk.regions]
    .sort((left, right) => compareStrings(left.regionId, right.regionId))
    .map((region) => ({
      regionId: region.regionId,
      displayName: region.displayName,
      disputeStatus: region.disputeStatus,
      disputeSource: region.disputeSource,
      gdeltCountryCodes: [...region.gdeltCountryCodes].sort(compareStrings),
    }));
}

export function buildUnmappedCatalog(crosswalk) {
  return [...crosswalk.unmappedGdeltCountryCodes]
    .sort((left, right) => compareStrings(left.code, right.code))
    .map((entry) => {
      const catalogEntry = {
        code: entry.code,
        lookupName: entry.lookupName,
        reason: entry.reason,
      };
      if (Object.hasOwn(entry, "evidence")) {
        catalogEntry.evidence = {
          kind: entry.evidence.kind,
          datasetSha256: entry.evidence.datasetSha256,
          observationCount: entry.evidence.observationCount,
          observedValue: entry.evidence.observedValue,
          description: entry.evidence.description,
        };
      }
      return catalogEntry;
    });
}

function validateToolMetadata({ geometryConfig, packageJson, packageLock }) {
  if (packageJson.version !== geometryConfig.toolchain.toolVersion) {
    fail("package.json.version", "не совпадает с geometry config");
  }
  if (packageJson.packageManager !== `npm@${geometryConfig.toolchain.npmVersion}`) {
    fail("package.json.packageManager", "не совпадает с geometry config");
  }
  if (packageJson.dependencies?.mapshaper !== geometryConfig.toolchain.mapshaperVersion) {
    fail("package.json.dependencies.mapshaper", "не совпадает с geometry config");
  }
  if (packageLock.lockfileVersion !== 3) {
    fail("package-lock.json.lockfileVersion", "ожидается lockfileVersion 3");
  }
  if (
    packageLock.packages?.[""]?.dependencies?.mapshaper !==
      geometryConfig.toolchain.mapshaperVersion ||
    packageLock.packages?.["node_modules/mapshaper"]?.version !==
      geometryConfig.toolchain.mapshaperVersion
  ) {
    fail("package-lock.json", "exact mapshaper version не совпадает с geometry config");
  }
}

function validateCrosswalkSourceReferences(crosswalk, sourceManifest) {
  const sourceIds = new Set(sourceManifest.sources.map((source) => source.sourceId));
  for (const sourceId of [
    crosswalk.naturalEarthSourceId,
    crosswalk.gdeltCountryLookupSourceId,
  ]) {
    if (!sourceIds.has(sourceId)) {
      fail(
        "country-crosswalk.json",
        `sourceId ${sourceId} отсутствует в source manifest`,
      );
    }
  }
}

function parseJson(bytes, label) {
  try {
    return JSON.parse(bytes.toString("utf8"));
  } catch (error) {
    throw new TypeError(`Не удалось разобрать ${label}: ${error.message}`, {
      cause: error,
    });
  }
}

function assertStrictObject(value, path, fields) {
  if (!isPlainObject(value)) {
    fail(path, "ожидается объект");
  }
  const expected = new Set(fields);
  for (const field of Object.keys(value)) {
    if (!expected.has(field)) {
      fail(`${path}.${field}`, "поле не входит в строгую schema");
    }
  }
  for (const field of fields) {
    if (!Object.hasOwn(value, field)) {
      fail(`${path}.${field}`, "обязательное поле отсутствует");
    }
  }
}

function assertPattern(value, pattern, path) {
  if (typeof value !== "string" || !pattern.test(value)) {
    fail(path, "значение имеет недопустимый формат");
  }
}

function assertExact(value, expected, path) {
  if (value !== expected) {
    fail(path, `ожидается ${JSON.stringify(expected)}, получено ${JSON.stringify(value)}`);
  }
}

function assertFiniteNumberInRange(value, minimum, maximum, path, minimumInclusive = true) {
  const minimumAccepted = minimumInclusive ? value >= minimum : value > minimum;
  if (!Number.isFinite(value) || !minimumAccepted || value > maximum) {
    fail(path, `ожидается конечное число в диапазоне ${minimumInclusive ? "[" : "("}${minimum}, ${maximum}]`);
  }
}

function assertSafeIntegerInRange(value, minimum, maximum, path) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    fail(path, `ожидается целое число в диапазоне [${minimum}, ${maximum}]`);
  }
}

function assertPositiveSafeInteger(value, path) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    fail(path, "ожидается положительное безопасное целое число");
  }
}

function assertCoordinatePrecision(value, path) {
  const precisionScale = Math.round(1 / value);
  const reciprocalError = Math.abs(precisionScale * value - 1);
  if (
    !Number.isSafeInteger(precisionScale) ||
    precisionScale <= 0 ||
    precisionScale > 100000 ||
    reciprocalError > Number.EPSILON * 8
  ) {
    fail(path, "ожидается десятичная сетка не точнее 0.00001");
  }
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fail(path, message) {
  throw new TypeError(`${path}: ${message}`);
}
