import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { encodeCanonicalJson } from "../src/canonical-json.mjs";
import { verifyArtifactPair } from "../src/geometry-artifact.mjs";
import {
  createTestContext,
  createTestCrosswalk,
  createTestGeometryConfig,
  deepClone,
  loadGeometryFixture,
  prepareTestArtifact,
} from "./geometry-test-fixtures.mjs";

test("Offline verifier принимает полную согласованную пару без raw inputs", async (t) => {
  const prepared = await prepareFixtureDirectory(t);

  const verification = await verifyArtifactPair({
    directoryPath: prepared.directoryPath,
    context: prepared.context,
    requireSelectedProfile: true,
  });

  assert.equal(verification.manifest.geometryVersion, "country-v1");
  assert.equal(verification.metrics.featureCount, 2);
  assert.ok(verification.metrics.vertexCount > 0);
});

test("Verifier отклоняет независимые изменения manifest", async (t) => {
  const mutations = [
    ["hash", (manifest) => { manifest.artifact.sha256 = "0".repeat(64); }],
    ["count", (manifest) => { manifest.artifact.vertexCount += 1; }],
    ["catalog", (manifest) => { manifest.regions.pop(); }],
    ["parameter", (manifest) => { manifest.parameters.coordinatePrecision = 0.5; }],
    ["version", (manifest) => { manifest.geometryVersion = "country-v2"; }],
    ["unknown field", (manifest) => { manifest.generatedAt = "2026-08-11T00:00:00Z"; }],
  ];

  for (const [label, mutate] of mutations) {
    await t.test(label, async (subtest) => {
      const prepared = await prepareFixtureDirectory(subtest);
      const changedManifest = deepClone(prepared.artifact.manifest);
      mutate(changedManifest);
      await writeFile(
        join(prepared.directoryPath, "manifest.json"),
        encodeCanonicalJson(changedManifest),
      );

      await assert.rejects(
        verifyArtifactPair({
          directoryPath: prepared.directoryPath,
          context: prepared.context,
        }),
        /manifest\.json/,
      );
    });
  }
});

test("Verifier отклоняет поврежденную или неподдерживаемую geometry", async (t) => {
  const mutations = [
    [
      "null geometry",
      (geojson) => { geojson.features[0].geometry = null; },
      /geometry: ожидается объект/,
    ],
    ["unsupported type", (geojson) => {
      geojson.features[0].geometry = {
        type: "LineString",
        coordinates: [[0, 0], [1, 1]],
      };
    }, /допустимы только "Polygon" и "MultiPolygon"/],
    ["empty polygon", (geojson) => {
      geojson.features[0].geometry = { type: "Polygon", coordinates: [] };
    }, /coordinates: ожидается непустой массив/],
    ["longitude outside range", (geojson) => {
      geojson.features[0].geometry.coordinates[0][0][0] = 181;
    }, /longitude должен быть конечным числом/],
    ["coordinate outside precision grid", (geojson) => {
      geojson.features[0].geometry.coordinates[0][1][0] += 0.0004;
    }, /координата не лежит на зафиксированной precision grid/],
    ["unclosed ring", (geojson) => {
      geojson.features[0].geometry.coordinates[0].at(-1)[0] = 0.5;
    }, /ring должен быть замкнут/],
    ["self-intersecting ring", (geojson) => {
      geojson.features[0].geometry = {
        type: "Polygon",
        coordinates: [[
          [0, 3],
          [-3, -1],
          [2, 2],
          [-2, 2],
          [3, -1],
          [0, 3],
        ]],
      };
    }, /ring самопересекается/],
    ["hole outside exterior ring", (geojson) => {
      geojson.features[0].geometry = {
        type: "Polygon",
        coordinates: [
          [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
          [[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]],
        ],
      };
    }, /hole должен лежать строго внутри exterior ring/],
    ["hole crossing exterior ring", (geojson) => {
      geojson.features[0].geometry = {
        type: "Polygon",
        coordinates: [
          [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
          [[3, 1], [5, 1], [5, 2], [3, 2], [3, 1]],
        ],
      };
    }, /hole пересекает exterior ring/],
    ["duplicate region", (geojson) => {
      geojson.features[1].properties.regionId = "country:alpha";
    }, /duplicate regionId/],
    ["unexpected property", (geojson) => {
      geojson.features[0].properties.gdeltCountryCodes = ["AA"];
    }, /gdeltCountryCodes: неожиданное поле/],
  ];

  for (const [label, mutate, expectedError] of mutations) {
    await t.test(label, async (subtest) => {
      const prepared = await prepareFixtureDirectory(subtest);
      const changedGeojson = deepClone(prepared.artifact.geojson);
      mutate(changedGeojson);
      await writeFile(
        join(prepared.directoryPath, "countries.geojson"),
        encodeCanonicalJson(changedGeojson),
      );

      await assert.rejects(
        verifyArtifactPair({
          directoryPath: prepared.directoryPath,
          context: prepared.context,
        }),
        expectedError,
      );
    });
  }
});

test("Manifest сохраняет полный ожидаемый catalog, включая регион без code и unmapped reason", async (t) => {
  const crosswalk = createTestCrosswalk();
  crosswalk.regions[1].gdeltCountryCodes = [];
  crosswalk.regions.reverse();
  crosswalk.unmappedGdeltCountryCodes = [
    {
      code: "BB",
      lookupName: "Beta",
      reason: "SOURCE_GEOMETRY_NOT_INDEPENDENT",
    },
  ];
  const context = createTestContext({ crosswalk });
  const prepared = await prepareFixtureDirectory(t, "country-v1", context);

  assert.deepEqual(prepared.artifact.manifest.regions, [
    {
      regionId: "country:alpha",
      displayName: "Alpha",
      disputeStatus: "STANDARD",
      disputeSource: null,
      gdeltCountryCodes: ["AA"],
    },
    {
      regionId: "country:beta",
      displayName: "Beta",
      disputeStatus: "DISPUTED_DE_FACTO",
      disputeSource: "natural-earth-test:feature-3",
      gdeltCountryCodes: [],
    },
  ]);
  assert.deepEqual(prepared.artifact.manifest.unmappedGdeltCountryCodes, [
    {
      code: "BB",
      lookupName: "Beta",
      reason: "SOURCE_GEOMETRY_NOT_INDEPENDENT",
    },
  ]);
  assert.equal(Object.hasOwn(prepared.artifact.manifest, "generatedAt"), false);
  await verifyArtifactPair({
    directoryPath: prepared.directoryPath,
    context,
  });
});

test("Manifest сохраняет полный ожидаемый source и tooling provenance", async (t) => {
  const prepared = await prepareFixtureDirectory(t);

  assert.deepEqual(prepared.artifact.manifest.provenance, {
    sourceManifest: {
      schemaVersion: "1",
      sha256: "cc4f1cfdc776c6c47e9937d7b4ee267abc5fcf937d9b842a3f2930603c2c0de6",
    },
    sources: [
      {
        sourceId: "gdelt-country-test",
        providerIdentity: "fixture:gdelt-country",
        repositoryCommit: null,
        url: "https://example.test/gdelt-country.tsv",
        license: {
          name: "Fixture",
          url: "https://example.test/license",
        },
        sha256: "c".repeat(64),
        maxBytes: 10000,
      },
      {
        sourceId: "natural-earth-test",
        providerIdentity: "fixture:natural-earth",
        repositoryCommit: "a".repeat(40),
        url: "https://example.test/natural-earth.geojson",
        license: {
          name: "Fixture",
          url: "https://example.test/license",
        },
        sha256: "b".repeat(64),
        maxBytes: 100000,
      },
    ],
    crosswalk: {
      schemaVersion: 1,
      version: "country-crosswalk-v1",
      sha256: "22c0346dd030fb4dcfb044e1de7ead25d3c6baa41b43597d7860181a6b72a0f3",
    },
    geometryConfig: {
      schemaVersion: 1,
      sha256: "5d7897bcd062406e98f7bcc5ef6750adc86d61ad11d925381db61b50c6428146",
    },
    tooling: {
      toolName: "@event-mosaic/map-geometry",
      toolVersion: "0.1.0",
      nodeVersion: "24.19.0",
      npmVersion: "11.17.0",
      mapshaperVersion: "0.7.51",
      packageJsonSha256: "12aa9af8dc9dacf816e7475a6f52c63f2a6e520af0fa042b37fd8bcfe375bcb1",
      packageLockSha256: "2b5af1a8224366818d9956922ccc0f3a8b9ac1108b494b91f77f7d7c80ce2cf7",
    },
  });
});

test("Verifier отклоняет stale manifest после изменения только версии crosswalk", async (t) => {
  const prepared = await prepareFixtureDirectory(t);
  const changedCrosswalk = deepClone(prepared.context.crosswalk);
  changedCrosswalk.crosswalkVersion = "country-crosswalk-v2";
  const changedContext = createTestContext({ crosswalk: changedCrosswalk });

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: changedContext,
    }),
    /manifest\.json не совпадает/,
  );
});

test("Verifier отклоняет валидное изменение GeoJSON со старым manifest", async (t) => {
  const prepared = await prepareFixtureDirectory(t);
  const changedGeojson = deepClone(prepared.artifact.geojson);
  const betaRing = changedGeojson.features[1].geometry.coordinates[0];
  for (const position of betaRing) {
    position[0] += 0.01;
  }
  await writeFile(
    join(prepared.directoryPath, "countries.geojson"),
    encodeCanonicalJson(changedGeojson),
  );

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: prepared.context,
    }),
    /manifest\.json не совпадает/,
  );
});

test("Verifier отклоняет пару, собранную с другим crosswalk", async (t) => {
  const prepared = await prepareFixtureDirectory(t);
  const changedCrosswalk = deepClone(prepared.context.crosswalk);
  changedCrosswalk.regions[0].displayName = "Alpha changed after build";
  const changedContext = createTestContext({ crosswalk: changedCrosswalk });

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: changedContext,
    }),
    /manifest\.json|displayName/,
  );
});

test("Verifier применяет structural budgets независимо от manifest", async (t) => {
  const prepared = await prepareFixtureDirectory(t);
  const strictConfig = createTestGeometryConfig();
  strictConfig.budgets.maxVertices = 1;
  const strictContext = createTestContext({ geometryConfig: strictConfig });

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: strictContext,
    }),
    /число вершин .* превышает structural budget 1/,
  );
});

test("Verifier отклоняет слишком большой GeoJSON до разбора файла", async (t) => {
  const prepared = await prepareFixtureDirectory(t);
  const maximumBytes = prepared.context.geometryConfig.budgets.maxGeojsonBytes;
  await writeFile(
    join(prepared.directoryPath, "countries.geojson"),
    Buffer.alloc(maximumBytes + 1, 0x20),
  );

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: prepared.context,
    }),
    new RegExp(`размер ${maximumBytes + 1}, предел чтения ${maximumBytes}`),
  );
});

test("Verifier требует совпадения имени каталога с geometryVersion", async (t) => {
  const prepared = await prepareFixtureDirectory(t, "country-v2");

  await assert.rejects(
    verifyArtifactPair({
      directoryPath: prepared.directoryPath,
      context: prepared.context,
    }),
    /Имя каталога country-v2 не совпадает с geometryVersion country-v1/,
  );
});

async function prepareFixtureDirectory(
  t,
  directoryName = "country-v1",
  context = createTestContext(),
) {
  const temporaryRoot = await mkdtemp(join(tmpdir(), "event-mosaic-geometry-"));
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));
  const directoryPath = join(temporaryRoot, "путь с пробелами", directoryName);
  await mkdir(directoryPath, { recursive: true });
  const fixture = await loadGeometryFixture();
  const artifact = await prepareTestArtifact({ fixture, context });
  await Promise.all([
    writeFile(join(directoryPath, "countries.geojson"), artifact.countriesGeojsonBytes),
    writeFile(join(directoryPath, "manifest.json"), artifact.manifestBytes),
  ]);
  return { temporaryRoot, directoryPath, fixture, context, artifact };
}
