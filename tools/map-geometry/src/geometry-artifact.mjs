import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  rename,
  rm,
} from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import {
  deterministicGzip,
  encodeCanonicalJson,
  parseCanonicalJson,
  sha256Hex,
} from "./canonical-json.mjs";
import { selectCandidateProfile } from "./geometry-config.mjs";
import { canonicalizeEngineFeatureCollection, validateCountryGeoJson } from "./geometry-validation.mjs";

const NO_FOLLOW = constants.O_NOFOLLOW ?? 0;
const ARTIFACT_FILE_NAMES = ["countries.geojson", "manifest.json"];
const MANIFEST_SCHEMA_VERSION = "1";
const DEFAULT_MAX_GEOJSON_READ_BYTES = 64 * 1024 * 1024;
const MAX_MANIFEST_READ_BYTES = 5 * 1024 * 1024;

export function createCountryGeometryArtifact({
  engineOutput,
  context,
  profileId = context.geometryConfig.selectedProfileId,
}) {
  const profile = selectCandidateProfile(context.geometryConfig, profileId);
  const geojson = canonicalizeEngineFeatureCollection({
    featureCollection: engineOutput,
    geometryVersion: context.geometryConfig.geometryVersion,
    geometrySource: context.geometryConfig.geometrySource,
    regions: context.regions,
    coordinatePrecision: profile.coordinatePrecision,
  });
  const geojsonBytes = encodeCanonicalJson(geojson);
  const validation = validateCountryGeoJson({
    featureCollection: geojson,
    geometryVersion: context.geometryConfig.geometryVersion,
    geometrySource: context.geometryConfig.geometrySource,
    regions: context.regions,
    coordinatePrecision: profile.coordinatePrecision,
  });
  const metrics = calculateArtifactMetrics({
    geojsonBytes,
    featureCount: validation.featureCount,
    vertexCount: validation.vertexCount,
    gzipConfiguration: context.geometryConfig.gzip,
  });
  assertStructuralBudgets(metrics, context.geometryConfig.budgets);
  const manifest = buildExpectedManifest({ context, profile, metrics });
  const manifestBytes = encodeCanonicalJson(manifest);

  return {
    profile,
    geojson,
    manifest,
    countriesGeojsonBytes: geojsonBytes,
    manifestBytes,
    metrics,
  };
}

export async function verifyArtifactPair({
  directoryPath,
  context,
  requireSelectedProfile = false,
  checkDirectoryVersion = true,
}) {
  const resolvedDirectory = resolve(directoryPath);
  const pair = await readArtifactPair(resolvedDirectory, {
    maxGeojsonBytes: context.geometryConfig.budgets.maxGeojsonBytes,
    maxManifestBytes: MAX_MANIFEST_READ_BYTES,
  });
  const manifest = parseCanonicalJson(pair.manifestBytes, "manifest.json");
  const profileId = readManifestProfileId(manifest);
  const profile = selectCandidateProfile(context.geometryConfig, profileId);
  if (
    requireSelectedProfile &&
    profile.profileId !== context.geometryConfig.selectedProfileId
  ) {
    throw new Error(
      `Публиковать можно только selected profile ${context.geometryConfig.selectedProfileId}`,
    );
  }
  if (
    checkDirectoryVersion &&
    basename(resolvedDirectory) !== context.geometryConfig.geometryVersion
  ) {
    throw new Error(
      `Имя каталога ${basename(resolvedDirectory)} не совпадает с geometryVersion ${context.geometryConfig.geometryVersion}`,
    );
  }

  const geojson = parseCanonicalJson(
    pair.countriesGeojsonBytes,
    "countries.geojson",
  );
  const validation = validateCountryGeoJson({
    featureCollection: geojson,
    geometryVersion: context.geometryConfig.geometryVersion,
    geometrySource: context.geometryConfig.geometrySource,
    regions: context.regions,
    coordinatePrecision: profile.coordinatePrecision,
  });
  const canonicalGeojsonBytes = encodeCanonicalJson(validation.canonicalValue);
  if (!pair.countriesGeojsonBytes.equals(canonicalGeojsonBytes)) {
    throw new Error("countries.geojson нарушает canonical feature/geometry ordering");
  }

  const metrics = calculateArtifactMetrics({
    geojsonBytes: pair.countriesGeojsonBytes,
    featureCount: validation.featureCount,
    vertexCount: validation.vertexCount,
    gzipConfiguration: context.geometryConfig.gzip,
  });
  assertStructuralBudgets(metrics, context.geometryConfig.budgets);
  const expectedManifest = buildExpectedManifest({ context, profile, metrics });
  const expectedManifestBytes = encodeCanonicalJson(expectedManifest);
  if (!pair.manifestBytes.equals(expectedManifestBytes)) {
    throw new Error(
      "manifest.json не совпадает с независимо вычисленным provenance, catalog, parameters или metrics",
    );
  }

  return {
    directoryPath: resolvedDirectory,
    profile,
    geojson: validation.canonicalValue,
    manifest: expectedManifest,
    metrics,
    countriesGeojsonBytes: pair.countriesGeojsonBytes,
    manifestBytes: pair.manifestBytes,
  };
}

export function resolveCandidateDirectory({ buildRoot, context, profileId }) {
  const profile = selectCandidateProfile(context.geometryConfig, profileId);
  const preparationFingerprint = sha256Hex(
    encodeCanonicalJson({
      geometryConfigSha256: context.hashes.geometryConfigSha256,
      sourceManifestSha256: context.hashes.sourceManifestSha256,
      crosswalkSha256: context.hashes.crosswalkSha256,
      packageJsonSha256: context.hashes.packageJsonSha256,
      packageLockSha256: context.hashes.packageLockSha256,
      profileId: profile.profileId,
    }),
  );
  return join(
    resolve(buildRoot),
    "candidates",
    profile.profileId,
    preparationFingerprint,
    context.geometryConfig.geometryVersion,
  );
}

export async function writeCandidateArtifact({
  directoryPath,
  artifact,
  context,
}) {
  const targetDirectory = resolve(directoryPath);
  const existing = await inspectExistingArtifact(targetDirectory, artifact);
  if (existing === "IDENTICAL") {
    await verifyArtifactPair({ directoryPath: targetDirectory, context });
    return { directoryPath: targetDirectory, reused: true };
  }
  if (existing !== "ABSENT") {
    throw new Error(
      `Candidate directory уже существует с partial или другими bytes: ${targetDirectory}`,
    );
  }

  const parentDirectory = dirname(targetDirectory);
  await ensureDirectoryChain(parentDirectory);
  const temporaryDirectory = join(
    parentDirectory,
    `.${basename(targetDirectory)}.candidate-${randomUUID()}`,
  );
  await mkdir(temporaryDirectory, { recursive: false });
  const temporaryDirectoryIdentity = await lstat(temporaryDirectory);
  try {
    await writeArtifactFiles(temporaryDirectory, artifact);
    await verifyArtifactPair({
      directoryPath: temporaryDirectory,
      context,
      checkDirectoryVersion: false,
    });
    await rename(temporaryDirectory, targetDirectory);
  } catch (error) {
    const recovered = await inspectExistingArtifact(targetDirectory, artifact);
    if (recovered !== "IDENTICAL") {
      throw error;
    }
  } finally {
    await removeOwnedTemporaryDirectory(
      temporaryDirectory,
      parentDirectory,
      temporaryDirectoryIdentity,
    );
  }
  await verifyArtifactPair({ directoryPath: targetDirectory, context });
  return { directoryPath: targetDirectory, reused: false };
}

export async function readArtifactPair(
  directoryPath,
  {
    maxGeojsonBytes = DEFAULT_MAX_GEOJSON_READ_BYTES,
    maxManifestBytes = MAX_MANIFEST_READ_BYTES,
  } = {},
) {
  const directoryStat = await lstat(directoryPath);
  if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink()) {
    throw new Error(`Artifact path не является обычным каталогом: ${directoryPath}`);
  }
  const entries = (await readdir(directoryPath)).sort(compareStrings);
  const expectedEntries = [...ARTIFACT_FILE_NAMES].sort(compareStrings);
  if (JSON.stringify(entries) !== JSON.stringify(expectedEntries)) {
    throw new Error(
      `Artifact directory должен содержать только ${ARTIFACT_FILE_NAMES.join(", ")}`,
    );
  }
  const [countriesGeojsonBytes, manifestBytes] = await Promise.all([
    readStableRegularFile(
      join(directoryPath, "countries.geojson"),
      maxGeojsonBytes,
    ),
    readStableRegularFile(
      join(directoryPath, "manifest.json"),
      maxManifestBytes,
    ),
  ]);
  return { countriesGeojsonBytes, manifestBytes };
}

function calculateArtifactMetrics({
  geojsonBytes,
  featureCount,
  vertexCount,
  gzipConfiguration,
}) {
  return {
    geojsonSha256: sha256Hex(geojsonBytes),
    geojsonBytes: geojsonBytes.length,
    gzipBytes: deterministicGzip(geojsonBytes, gzipConfiguration).length,
    featureCount,
    vertexCount,
  };
}

function assertStructuralBudgets(metrics, budgets) {
  const checks = [
    [metrics.geojsonBytes, budgets.maxGeojsonBytes, "обычный размер"],
    [metrics.gzipBytes, budgets.maxGzipBytes, "gzip-размер"],
    [metrics.vertexCount, budgets.maxVertices, "число вершин"],
  ];
  for (const [actual, maximum, label] of checks) {
    if (actual > maximum) {
      throw new Error(`${label} ${actual} превышает structural budget ${maximum}`);
    }
  }
}

function buildExpectedManifest({ context, profile, metrics }) {
  const config = context.geometryConfig;
  return {
    manifestSchemaVersion: MANIFEST_SCHEMA_VERSION,
    geometryVersion: config.geometryVersion,
    profileId: profile.profileId,
    provenance: {
      sourceManifest: {
        schemaVersion: context.sourceManifest.manifestSchemaVersion,
        sha256: context.hashes.sourceManifestSha256,
      },
      sources: [...context.sourceManifest.sources]
        .sort((left, right) => compareStrings(left.sourceId, right.sourceId))
        .map((source) => ({
          sourceId: source.sourceId,
          providerIdentity: source.providerIdentity,
          repositoryCommit: source.repositoryCommit,
          url: source.url,
          license: {
            name: source.license.name,
            url: source.license.url,
          },
          sha256: source.sha256,
          maxBytes: source.maxBytes,
        })),
      crosswalk: {
        schemaVersion: context.crosswalk.crosswalkSchemaVersion,
        version: context.crosswalk.crosswalkVersion,
        sha256: context.hashes.crosswalkSha256,
      },
      geometryConfig: {
        schemaVersion: config.geometryConfigSchemaVersion,
        sha256: context.hashes.geometryConfigSha256,
      },
      tooling: {
        toolName: context.packageJson.name,
        toolVersion: config.toolchain.toolVersion,
        nodeVersion: config.toolchain.nodeVersion,
        npmVersion: config.toolchain.npmVersion,
        mapshaperVersion: config.toolchain.mapshaperVersion,
        packageJsonSha256: context.hashes.packageJsonSha256,
        packageLockSha256: context.hashes.packageLockSha256,
      },
    },
    parameters: {
      selection: "exact-crosswalk-feature-ids",
      dissolve: { field: "regionId" },
      simplification: {
        method: profile.simplificationMethod,
        retainPercent: profile.retainPercent,
        keepShapes: profile.keepShapes,
      },
      coordinatePrecision: profile.coordinatePrecision,
      propertyProjection: [
        "regionId",
        "displayName",
        "disputeStatus",
        "geometrySource",
      ],
      canonicalization: {
        schema: "event-mosaic-country-geojson-v1",
        encoding: "UTF-8",
        lineEnding: "LF",
        trailingNewline: true,
      },
      gzip: {
        level: config.gzip.level,
        mtime: config.gzip.mtime,
      },
    },
    artifact: {
      fileName: "countries.geojson",
      sha256: metrics.geojsonSha256,
      bytes: metrics.geojsonBytes,
      gzipBytes: metrics.gzipBytes,
      featureCount: metrics.featureCount,
      vertexCount: metrics.vertexCount,
    },
    budgets: {
      maxGeojsonBytes: config.budgets.maxGeojsonBytes,
      maxGzipBytes: config.budgets.maxGzipBytes,
      maxVertices: config.budgets.maxVertices,
    },
    regions: context.regions,
    unmappedGdeltCountryCodes: context.unmappedGdeltCountryCodes,
  };
}

function readManifestProfileId(manifest) {
  if (typeof manifest !== "object" || manifest === null || Array.isArray(manifest)) {
    throw new TypeError("manifest.json: ожидается объект");
  }
  if (typeof manifest.profileId !== "string") {
    throw new TypeError("manifest.json.profileId: ожидается строка");
  }
  return manifest.profileId;
}

async function writeArtifactFiles(directoryPath, artifact) {
  await Promise.all([
    writeExclusiveFile(
      join(directoryPath, "countries.geojson"),
      artifact.countriesGeojsonBytes,
    ),
    writeExclusiveFile(join(directoryPath, "manifest.json"), artifact.manifestBytes),
  ]);
}

async function writeExclusiveFile(filePath, bytes) {
  const fileHandle = await open(filePath, "wx", 0o644);
  try {
    await fileHandle.writeFile(bytes);
    await fileHandle.sync();
  } finally {
    await fileHandle.close();
  }
}

async function inspectExistingArtifact(directoryPath, artifact) {
  try {
    const pair = await readArtifactPair(directoryPath, {
      maxGeojsonBytes: artifact.countriesGeojsonBytes.length,
      maxManifestBytes: artifact.manifestBytes.length,
    });
    return pair.countriesGeojsonBytes.equals(artifact.countriesGeojsonBytes) &&
      pair.manifestBytes.equals(artifact.manifestBytes)
      ? "IDENTICAL"
      : "DIFFERENT";
  } catch (error) {
    if (error?.code === "ENOENT") {
      return "ABSENT";
    }
    return "PARTIAL_OR_INVALID";
  }
}

async function readStableRegularFile(filePath, maximumBytes) {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes <= 0) {
    throw new TypeError("Artifact read limit должен быть положительным целым числом");
  }
  const pathStatBefore = await lstat(filePath);
  if (!pathStatBefore.isFile() || pathStatBefore.isSymbolicLink()) {
    throw new Error(`Artifact member не является обычным файлом: ${filePath}`);
  }
  assertFileWithinReadLimit(pathStatBefore, maximumBytes, filePath);
  const fileHandle = await open(filePath, constants.O_RDONLY | NO_FOLLOW);
  try {
    const handleStatBefore = await fileHandle.stat();
    assertSameFileIdentity(pathStatBefore, handleStatBefore, filePath);
    assertFileWithinReadLimit(handleStatBefore, maximumBytes, filePath);
    const bytes = await readExactSnapshot(fileHandle, handleStatBefore.size, filePath);
    const handleStatAfter = await fileHandle.stat();
    assertSameFileSnapshot(handleStatBefore, handleStatAfter, filePath);
    const pathStatAfter = await lstat(filePath);
    assertSameFileSnapshot(handleStatAfter, pathStatAfter, filePath);
    return bytes;
  } finally {
    await fileHandle.close();
  }
}

async function readExactSnapshot(fileHandle, expectedSize, filePath) {
  const bytes = Buffer.allocUnsafe(expectedSize);
  let offset = 0;
  while (offset < expectedSize) {
    const { bytesRead } = await fileHandle.read(
      bytes,
      offset,
      expectedSize - offset,
      offset,
    );
    if (bytesRead === 0) {
      throw new Error(`Artifact file сократился во время чтения: ${filePath}`);
    }
    offset += bytesRead;
  }
  const probe = Buffer.allocUnsafe(1);
  const { bytesRead: extraBytesRead } = await fileHandle.read(
    probe,
    0,
    1,
    expectedSize,
  );
  if (extraBytesRead !== 0) {
    throw new Error(`Artifact file вырос во время чтения: ${filePath}`);
  }
  return bytes;
}

function assertFileWithinReadLimit(stat, maximumBytes, filePath) {
  if (stat.size > maximumBytes) {
    throw new Error(
      `Artifact file ${filePath} имеет размер ${stat.size}, предел чтения ${maximumBytes}`,
    );
  }
}

async function ensureDirectoryChain(directoryPath) {
  const resolved = resolve(directoryPath);
  const parent = dirname(resolved);
  if (parent !== resolved) {
    try {
      await lstat(parent);
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw error;
      }
      await ensureDirectoryChain(parent);
    }
  }
  try {
    await mkdir(resolved, { recursive: false });
  } catch (error) {
    if (error?.code !== "EEXIST") {
      throw error;
    }
  }
  const stat = await lstat(resolved);
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    throw new Error(`Ожидается обычный каталог без ссылок: ${resolved}`);
  }
  const actual = await realpath(resolved);
  if (resolve(actual) !== resolved) {
    throw new Error(`Каталог разрешается в неожиданный путь: ${resolved}`);
  }
}

async function removeOwnedTemporaryDirectory(
  temporaryDirectory,
  parentDirectory,
  expectedIdentity,
) {
  if (
    dirname(temporaryDirectory) !== parentDirectory ||
    !basename(temporaryDirectory).includes(".candidate-")
  ) {
    throw new Error("Отказ очищать каталог, который не похож на собственный candidate temp");
  }
  let currentIdentity;
  try {
    currentIdentity = await lstat(temporaryDirectory);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return;
    }
    throw error;
  }
  if (
    !currentIdentity.isDirectory() ||
    currentIdentity.isSymbolicLink() ||
    !sameFileIdentity(expectedIdentity, currentIdentity)
  ) {
    throw new Error("Candidate temp был заменен; чужой путь оставлен без удаления");
  }
  try {
    await rm(temporaryDirectory, { recursive: true, force: false });
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

function sameFileIdentity(expected, actual) {
  const sameInode = expected.ino !== 0 && expected.ino === actual.ino;
  const comparableDevice = expected.dev !== 0 && actual.dev !== 0;
  return sameInode && (!comparableDevice || expected.dev === actual.dev);
}

function assertSameFileIdentity(expected, actual, filePath) {
  if (!sameFileIdentity(expected, actual)) {
    throw new Error(`Artifact file был заменен во время чтения: ${filePath}`);
  }
}

function assertSameFileSnapshot(expected, actual, filePath) {
  assertSameFileIdentity(expected, actual, filePath);
  if (expected.size !== actual.size || expected.mtimeMs !== actual.mtimeMs) {
    throw new Error(`Artifact file изменился во время чтения: ${filePath}`);
  }
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}
