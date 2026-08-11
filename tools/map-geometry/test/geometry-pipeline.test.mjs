import assert from "node:assert/strict";
import test from "node:test";

import { prepareCountryGeometryArtifact } from "../src/geometry-pipeline.mjs";
import {
  createTestContext,
  deepClone,
  loadGeometryFixture,
  prepareTestArtifact,
  validateTestCrosswalk,
} from "./geometry-test-fixtures.mjs";

test("Повтор подготовки дает те же bytes и объединяет части одного региона", async () => {
  const fixture = await loadGeometryFixture();
  const context = createTestContext();
  const first = await prepareTestArtifact({ fixture, context });

  const reorderedFixture = {
    ...fixture,
    naturalEarthFeatureCollection: {
      ...fixture.naturalEarthFeatureCollection,
      features: [...fixture.naturalEarthFeatureCollection.features].reverse(),
    },
  };
  const second = await prepareTestArtifact({
    fixture: reorderedFixture,
    context,
  });

  assert.deepEqual(first.countriesGeojsonBytes, second.countriesGeojsonBytes);
  assert.deepEqual(first.manifestBytes, second.manifestBytes);
  assert.deepEqual(
    first.geojson.features.map((feature) => feature.properties.regionId),
    ["country:alpha", "country:beta"],
  );
  const alpha = first.geojson.features[0];
  assert.equal(alpha.geometry.type, "Polygon");
  assert.deepEqual(Object.keys(alpha.properties), [
    "regionId",
    "displayName",
    "disputeStatus",
    "geometrySource",
  ]);
});

test("Смена параметра подготовки создает другой воспроизводимый manifest", async () => {
  const fixture = await loadGeometryFixture();
  const context = createTestContext();
  const baseline = await prepareTestArtifact({ fixture, context });
  const changed = await prepareTestArtifact({
    fixture,
    context,
    profileId: "retain-80-p3",
  });

  assert.notDeepEqual(baseline.manifestBytes, changed.manifestBytes);
  assert.equal(changed.manifest.profileId, "retain-80-p3");
  assert.equal(changed.manifest.parameters.simplification.retainPercent, 80);
});

test("Pipeline передает mapshaper точную последовательность и только regionId", async () => {
  const fixture = await loadGeometryFixture();
  const context = createTestContext();
  const validatedCrosswalk = validateTestCrosswalk({
    context,
    naturalEarthFeatureCollection: fixture.naturalEarthFeatureCollection,
    gdeltCountryLookupTsv: fixture.gdeltCountryLookupTsv,
  });
  const importedMapshaper = await import("mapshaper");
  const mapshaper = importedMapshaper.default ?? importedMapshaper;
  let callCount = 0;

  const artifact = await prepareCountryGeometryArtifact({
    naturalEarthFeatureCollection: fixture.naturalEarthFeatureCollection,
    validatedCrosswalk,
    context,
    profileId: "retain-20-p4",
    applyCommands: async (commands, inputs) => {
      callCount += 1;
      assert.deepEqual(commands, [
        "-i",
        "verified-input.geojson",
        "-dissolve",
        "regionId",
        "-simplify",
        "20%",
        "weighted",
        "keep-shapes",
        "-o",
        "prepared-output.geojson",
        "format=geojson",
        "precision=0.0001",
      ]);
      assert.deepEqual(Object.keys(inputs), ["verified-input.geojson"]);
      const engineInput = JSON.parse(inputs["verified-input.geojson"].toString("utf8"));
      assert.deepEqual(
        engineInput.features.map((feature) => Object.keys(feature.properties)),
        [["regionId"], ["regionId"], ["regionId"]],
      );
      return mapshaper.applyCommands(commands, inputs);
    },
  });

  assert.equal(callCount, 1);
  assert.equal(artifact.manifest.parameters.coordinatePrecision, 0.0001);
  assert.equal(artifact.manifest.parameters.simplification.keepShapes, true);
});

test("Смена crosswalk отражается и в GeoJSON, и в manifest", async () => {
  const fixture = await loadGeometryFixture();
  const baselineContext = createTestContext();
  const baseline = await prepareTestArtifact({
    fixture,
    context: baselineContext,
  });
  const changedCrosswalk = deepClone(baselineContext.crosswalk);
  changedCrosswalk.regions[0].displayName = "Alpha updated";
  const changedContext = createTestContext({ crosswalk: changedCrosswalk });
  const changed = await prepareTestArtifact({
    fixture,
    context: changedContext,
  });

  assert.notDeepEqual(baseline.countriesGeojsonBytes, changed.countriesGeojsonBytes);
  assert.notDeepEqual(baseline.manifestBytes, changed.manifestBytes);
  assert.equal(changed.geojson.features[0].properties.displayName, "Alpha updated");
});

test("Поврежденная source geometry отклоняется до вызова mapshaper", async () => {
  const fixture = await loadGeometryFixture();
  const invalidNaturalEarth = deepClone(fixture.naturalEarthFeatureCollection);
  invalidNaturalEarth.features[0].geometry = {
    type: "LineString",
    coordinates: [[0, 0], [1, 1]],
  };
  const context = createTestContext();
  const validatedCrosswalk = validateTestCrosswalk({
    context,
    naturalEarthFeatureCollection: invalidNaturalEarth,
    gdeltCountryLookupTsv: fixture.gdeltCountryLookupTsv,
  });
  let engineCalled = false;

  await assert.rejects(
    prepareCountryGeometryArtifact({
      naturalEarthFeatureCollection: invalidNaturalEarth,
      validatedCrosswalk,
      context,
      applyCommands: async () => {
        engineCalled = true;
        throw new Error("mapshaper не должен вызываться");
      },
    }),
    /допустимы только "Polygon" и "MultiPolygon"/,
  );
  assert.equal(engineCalled, false);
});
