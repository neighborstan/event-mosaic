import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  validateCountryCrosswalk,
  validateCountryCrosswalkStructure,
} from "./country-crosswalk.mjs";
import {
  downloadSources,
  readVerifiedSourceBuffer,
  verifyDownloadedSources,
} from "./download-sources.mjs";
import { readSourceManifest } from "./source-manifest.mjs";

const TOOL_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const REPOSITORY_ROOT = resolve(TOOL_ROOT, "..", "..");
const BUILD_ROOT = join(REPOSITORY_ROOT, "build", "map-geometry");
const SOURCE_MANIFEST_PATH = join(TOOL_ROOT, "data", "source-manifest.json");
const CROSSWALK_PATH = join(TOOL_ROOT, "data", "country-crosswalk.json");

export async function runMapGeometryCommand(command) {
  const configuration = await readAndValidateConfiguration();

  if (command === "prepare") {
    const sources = await downloadSources({
      manifest: configuration.manifest,
      repositoryRoot: REPOSITORY_ROOT,
      buildRoot: BUILD_ROOT,
    });
    return validateInputsAndSummarize(command, configuration, sources);
  }

  if (command === "verify") {
    const sources = await verifyDownloadedSources({
      manifest: configuration.manifest,
      repositoryRoot: REPOSITORY_ROOT,
      buildRoot: BUILD_ROOT,
    });
    return validateInputsAndSummarize(command, configuration, sources);
  }

  if (command === "publish") {
    throw new Error(
      "Публикация еще недоступна: offline verifier и create-only publisher относятся к MAP-01B",
    );
  }

  throw new Error(`Неизвестная команда map geometry: ${String(command)}`);
}

export async function runCli(command) {
  try {
    const summary = await runMapGeometryCommand(command);
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`Ошибка map geometry: ${message}\n`);
    process.exitCode = 1;
  }
}

async function readAndValidateConfiguration() {
  const [manifest, crosswalk] = await Promise.all([
    readSourceManifest(SOURCE_MANIFEST_PATH),
    readJson(CROSSWALK_PATH, "country crosswalk"),
  ]);
  validateCountryCrosswalkStructure(crosswalk);
  const declaredSourceIds = new Set(
    manifest.sources.map((source) => source.sourceId),
  );

  for (const sourceId of [
    crosswalk.naturalEarthSourceId,
    crosswalk.gdeltCountryLookupSourceId,
  ]) {
    if (!declaredSourceIds.has(sourceId)) {
      throw new Error(
        `Country crosswalk ссылается на sourceId "${sourceId}", которого нет в source manifest`,
      );
    }
  }

  return { manifest, crosswalk };
}

async function validateInputsAndSummarize(command, configuration, sources) {
  const verifiedSourceIds = new Set(
    sources.map((source) => source.sourceId),
  );
  if (
    !verifiedSourceIds.has(configuration.crosswalk.naturalEarthSourceId) ||
    !verifiedSourceIds.has(configuration.crosswalk.gdeltCountryLookupSourceId)
  ) {
    throw new Error("Проверенные source inputs не покрывают country crosswalk");
  }

  const [naturalEarthBuffer, gdeltCountryLookupBuffer] =
    await Promise.all([
      readVerifiedSourceBuffer({
        manifest: configuration.manifest,
        repositoryRoot: REPOSITORY_ROOT,
        buildRoot: BUILD_ROOT,
        sourceId: configuration.crosswalk.naturalEarthSourceId,
      }),
      readVerifiedSourceBuffer({
        manifest: configuration.manifest,
        repositoryRoot: REPOSITORY_ROOT,
        buildRoot: BUILD_ROOT,
        sourceId: configuration.crosswalk.gdeltCountryLookupSourceId,
      }),
    ]);
  const naturalEarthFeatureCollection = parseJsonBuffer(
    naturalEarthBuffer,
    "Natural Earth GeoJSON",
  );
  const gdeltCountryLookupTsv = gdeltCountryLookupBuffer.toString("utf8");
  const crosswalk = validateCountryCrosswalk({
    crosswalk: configuration.crosswalk,
    naturalEarthFeatureCollection,
    gdeltCountryLookupTsv,
  });

  return {
    command,
    buildRoot: BUILD_ROOT,
    sources: sources.map(({ sourceId, fileName, bytes, sha256, reused }) => ({
      sourceId,
      fileName,
      bytes,
      sha256,
      reused,
    })),
    crosswalk: {
      version: configuration.crosswalk.crosswalkVersion,
      regions: crosswalk.regionById.size,
      naturalEarthFeatures: crosswalk.naturalEarthFeatureIds.size,
      mappedGdeltCountryCodes: crosswalk.regionByGdeltCountryCode.size,
      unmappedGdeltCountryCodes:
        crosswalk.unmappedGdeltCountryCodeByCode.size,
    },
  };
}

async function readJson(filePath, label) {
  try {
    return JSON.parse(await readFile(filePath, "utf8"));
  } catch (error) {
    throw new Error(`Не удалось прочитать ${label}: ${error.message}`, {
      cause: error,
    });
  }
}

function parseJsonBuffer(buffer, label) {
  try {
    return JSON.parse(buffer.toString("utf8"));
  } catch (error) {
    throw new Error(`Не удалось разобрать ${label}: ${error.message}`, {
      cause: error,
    });
  }
}
