import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { validateCountryCrosswalk } from "../src/country-crosswalk.mjs";
import { createGeometryProjectContext } from "../src/geometry-config.mjs";
import { prepareCountryGeometryArtifact } from "../src/geometry-pipeline.mjs";

const FIXTURE_ROOT = join(
  dirname(fileURLToPath(import.meta.url)),
  "fixtures",
  "geometry",
);

export async function loadGeometryFixture() {
  const [naturalEarthText, gdeltCountryLookupTsv] = await Promise.all([
    readFile(join(FIXTURE_ROOT, "natural-earth.geojson"), "utf8"),
    readFile(join(FIXTURE_ROOT, "gdelt-country-lookup.tsv"), "utf8"),
  ]);
  return {
    naturalEarthFeatureCollection: JSON.parse(naturalEarthText),
    gdeltCountryLookupTsv,
  };
}

export function createTestCrosswalk() {
  return {
    crosswalkSchemaVersion: 1,
    crosswalkVersion: "country-crosswalk-v1",
    naturalEarthSourceId: "natural-earth-test",
    gdeltCountryLookupSourceId: "gdelt-country-test",
    naturalEarthFeatureKey: "NE_ID",
    naturalEarthFeatureCoverage: "ALL",
    regions: [
      {
        regionId: "country:alpha",
        displayName: "Alpha",
        naturalEarthFeatureIds: ["2", "1"],
        gdeltCountryCodes: ["AA"],
        disputeStatus: "STANDARD",
        disputeSource: null,
      },
      {
        regionId: "country:beta",
        displayName: "Beta",
        naturalEarthFeatureIds: ["3"],
        gdeltCountryCodes: ["BB"],
        disputeStatus: "DISPUTED_DE_FACTO",
        disputeSource: "natural-earth-test:feature-3",
      },
    ],
    mappedLegacyGdeltCountryCodeEvidence: [],
    unmappedGdeltCountryCodes: [],
  };
}

export function createTestGeometryConfig() {
  return {
    geometryConfigSchemaVersion: 1,
    geometryVersion: "country-v1",
    geometrySource: "natural-earth-10m",
    toolchain: {
      toolVersion: "0.1.0",
      nodeVersion: "24.19.0",
      npmVersion: "11.17.0",
      mapshaperVersion: "0.7.51",
    },
    candidateProfiles: [
      {
        profileId: "retain-90-p3",
        simplificationMethod: "weighted",
        retainPercent: 90,
        keepShapes: true,
        coordinatePrecision: 0.001,
      },
      {
        profileId: "retain-80-p3",
        simplificationMethod: "weighted",
        retainPercent: 80,
        keepShapes: true,
        coordinatePrecision: 0.001,
      },
      {
        profileId: "retain-70-p3",
        simplificationMethod: "weighted",
        retainPercent: 70,
        keepShapes: true,
        coordinatePrecision: 0.001,
      },
      {
        profileId: "retain-20-p4",
        simplificationMethod: "weighted",
        retainPercent: 20,
        keepShapes: true,
        coordinatePrecision: 0.0001,
      },
    ],
    selectedProfileId: "retain-90-p3",
    gzip: { level: 9, mtime: 0 },
    budgets: {
      maxGeojsonBytes: 100000,
      maxGzipBytes: 100000,
      maxVertices: 10000,
    },
  };
}

export function createTestContext({
  geometryConfig = createTestGeometryConfig(),
  crosswalk = createTestCrosswalk(),
} = {}) {
  const sourceManifest = {
    manifestSchemaVersion: "1",
    sources: [
      {
        sourceId: "natural-earth-test",
        fileName: "natural-earth.geojson",
        url: "https://example.test/natural-earth.geojson",
        providerIdentity: "fixture:natural-earth",
        repositoryCommit: "a".repeat(40),
        license: {
          name: "Fixture",
          url: "https://example.test/license",
        },
        sha256: "b".repeat(64),
        maxBytes: 100000,
        contentTypes: ["application/geo+json"],
      },
      {
        sourceId: "gdelt-country-test",
        fileName: "gdelt-country.tsv",
        url: "https://example.test/gdelt-country.tsv",
        providerIdentity: "fixture:gdelt-country",
        repositoryCommit: null,
        license: {
          name: "Fixture",
          url: "https://example.test/license",
        },
        sha256: "c".repeat(64),
        maxBytes: 10000,
        contentTypes: ["text/plain"],
      },
    ],
  };
  const packageJson = {
    name: "@event-mosaic/map-geometry",
    version: "0.1.0",
    packageManager: "npm@11.17.0",
    dependencies: { mapshaper: "0.7.51" },
  };
  const packageLock = {
    name: "@event-mosaic/map-geometry",
    version: "0.1.0",
    lockfileVersion: 3,
    packages: {
      "": { dependencies: { mapshaper: "0.7.51" } },
      "node_modules/mapshaper": { version: "0.7.51" },
    },
  };

  return createGeometryProjectContext({
    geometryConfig,
    geometryConfigBytes: jsonBytes(geometryConfig),
    sourceManifest,
    sourceManifestBytes: jsonBytes(sourceManifest),
    crosswalk,
    crosswalkBytes: jsonBytes(crosswalk),
    packageJson,
    packageJsonBytes: jsonBytes(packageJson),
    packageLock,
    packageLockBytes: jsonBytes(packageLock),
  });
}

export function validateTestCrosswalk({
  context,
  naturalEarthFeatureCollection,
  gdeltCountryLookupTsv,
}) {
  return validateCountryCrosswalk({
    crosswalk: context.crosswalk,
    naturalEarthFeatureCollection,
    gdeltCountryLookupTsv,
  });
}

export async function prepareTestArtifact({
  fixture,
  context,
  profileId = context.geometryConfig.selectedProfileId,
}) {
  const validatedCrosswalk = validateTestCrosswalk({
    context,
    naturalEarthFeatureCollection: fixture.naturalEarthFeatureCollection,
    gdeltCountryLookupTsv: fixture.gdeltCountryLookupTsv,
  });
  return prepareCountryGeometryArtifact({
    naturalEarthFeatureCollection: fixture.naturalEarthFeatureCollection,
    validatedCrosswalk,
    context,
    profileId,
  });
}

export function deepClone(value) {
  return structuredClone(value);
}

function jsonBytes(value) {
  return Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");
}
