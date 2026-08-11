import { createRequire } from "node:module";

import { createCountryGeometryArtifact } from "./geometry-artifact.mjs";
import { selectCandidateProfile } from "./geometry-config.mjs";
import { canonicalizeGeometry } from "./geometry-validation.mjs";

const require = createRequire(import.meta.url);

export async function prepareCountryGeometryArtifact({
  naturalEarthFeatureCollection,
  validatedCrosswalk,
  context,
  profileId = context.geometryConfig.selectedProfileId,
  applyCommands,
}) {
  const profile = selectCandidateProfile(context.geometryConfig, profileId);
  const engineInput = buildEngineInput({
    naturalEarthFeatureCollection,
    validatedCrosswalk,
    regions: context.crosswalk.regions,
  });
  const mapshaperApplyCommands = applyCommands ?? await loadMapshaperApplyCommands(
    context.geometryConfig.toolchain.mapshaperVersion,
  );
  const engineOutput = await runMapshaperPipeline({
    engineInput,
    profile,
    applyCommands: mapshaperApplyCommands,
  });
  return createCountryGeometryArtifact({
    engineOutput,
    context,
    profileId: profile.profileId,
  });
}

function buildEngineInput({
  naturalEarthFeatureCollection,
  validatedCrosswalk,
  regions,
}) {
  const featureById = new Map();
  for (const [index, feature] of naturalEarthFeatureCollection.features.entries()) {
    const rawFeatureId = feature.properties?.[validatedCrosswalk.crosswalk.naturalEarthFeatureKey];
    const featureId = toCanonicalFeatureId(rawFeatureId, `naturalEarth.features[${index}]`);
    if (featureById.has(featureId)) {
      throw new TypeError(`Natural Earth source содержит duplicate feature ${featureId}`);
    }
    featureById.set(featureId, feature);
  }

  const selectedFeatures = [];
  for (const region of [...regions].sort((left, right) =>
    compareStrings(left.regionId, right.regionId))) {
    for (const featureId of [...region.naturalEarthFeatureIds].sort(compareStrings)) {
      const sourceFeature = featureById.get(featureId);
      if (sourceFeature === undefined) {
        throw new TypeError(
          `Natural Earth feature ${featureId} региона ${region.regionId} отсутствует`,
        );
      }
      selectedFeatures.push({
        type: "Feature",
        properties: { regionId: region.regionId },
        geometry: canonicalizeGeometry(
          sourceFeature.geometry,
          `Natural Earth feature ${featureId}.geometry`,
        ),
      });
    }
  }
  return { type: "FeatureCollection", features: selectedFeatures };
}

async function runMapshaperPipeline({ engineInput, profile, applyCommands }) {
  const inputName = "verified-input.geojson";
  const outputName = "prepared-output.geojson";
  const commands = [
    "-i",
    inputName,
    "-dissolve",
    "regionId",
    "-simplify",
    `${profile.retainPercent}%`,
    profile.simplificationMethod,
    "keep-shapes",
    "-o",
    outputName,
    "format=geojson",
    `precision=${profile.coordinatePrecision}`,
  ];
  let output;
  try {
    output = await applyCommands(commands, {
      [inputName]: Buffer.from(JSON.stringify(engineInput), "utf8"),
    });
  } catch (error) {
    throw new Error(`mapshaper не подготовил geometry: ${error.message}`, {
      cause: error,
    });
  }
  const outputBytes = output?.[outputName];
  if (!Buffer.isBuffer(outputBytes) && typeof outputBytes !== "string") {
    throw new Error("mapshaper не вернул ожидаемый GeoJSON Buffer");
  }
  try {
    return JSON.parse(Buffer.from(outputBytes).toString("utf8"));
  } catch (error) {
    throw new Error(`mapshaper вернул поврежденный GeoJSON: ${error.message}`, {
      cause: error,
    });
  }
}

async function loadMapshaperApplyCommands(expectedVersion) {
  const installedPackage = require("mapshaper/package.json");
  if (installedPackage.version !== expectedVersion) {
    throw new Error(
      `Установлен mapshaper ${String(installedPackage.version)}, ожидается ${expectedVersion}`,
    );
  }
  const imported = await import("mapshaper");
  const mapshaper = imported.default ?? imported;
  if (typeof mapshaper.applyCommands !== "function") {
    throw new Error("Установленный mapshaper не предоставляет applyCommands()");
  }
  return mapshaper.applyCommands.bind(mapshaper);
}

function toCanonicalFeatureId(value, path) {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return String(value);
  }
  if (typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value)) {
    return value;
  }
  throw new TypeError(`${path}: NE_ID должен быть каноническим неотрицательным целым`);
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}
