import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { validateCountryCrosswalk } from "./country-crosswalk.mjs";
import {
  downloadSources,
  readVerifiedSourceBuffer,
} from "./download-sources.mjs";
import {
  resolveCandidateDirectory,
  verifyArtifactPair,
  writeCandidateArtifact,
} from "./geometry-artifact.mjs";
import {
  assertSupportedRuntime,
  readGeometryProjectContext,
  selectCandidateProfile,
} from "./geometry-config.mjs";
import { prepareCountryGeometryArtifact } from "./geometry-pipeline.mjs";
import { publishGeometryArtifact } from "./geometry-publisher.mjs";

const TOOL_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const REPOSITORY_ROOT = resolve(TOOL_ROOT, "..", "..");
const BUILD_ROOT = join(REPOSITORY_ROOT, "build", "map-geometry");
const STATIC_GEOMETRY_ROOT = join(
  REPOSITORY_ROOT,
  "src",
  "main",
  "resources",
  "static",
  "map",
  "geometry",
);

export async function runMapGeometryCommand(command, rawArguments = []) {
  const context = await readGeometryProjectContext(TOOL_ROOT);
  const options = parseCliArguments(command, rawArguments);

  if (command === "prepare") {
    assertSupportedRuntime(context.geometryConfig);
    const profile = selectCandidateProfile(
      context.geometryConfig,
      options.profileId,
    );
    const sources = await downloadSources({
      manifest: context.sourceManifest,
      repositoryRoot: REPOSITORY_ROOT,
      buildRoot: BUILD_ROOT,
    });
    const [naturalEarthBuffer, gdeltCountryLookupBuffer] = await Promise.all([
      readVerifiedSourceBuffer({
        manifest: context.sourceManifest,
        repositoryRoot: REPOSITORY_ROOT,
        buildRoot: BUILD_ROOT,
        sourceId: context.crosswalk.naturalEarthSourceId,
      }),
      readVerifiedSourceBuffer({
        manifest: context.sourceManifest,
        repositoryRoot: REPOSITORY_ROOT,
        buildRoot: BUILD_ROOT,
        sourceId: context.crosswalk.gdeltCountryLookupSourceId,
      }),
    ]);
    const naturalEarthFeatureCollection = parseJsonBuffer(
      naturalEarthBuffer,
      "Natural Earth GeoJSON",
    );
    const validatedCrosswalk = validateCountryCrosswalk({
      crosswalk: context.crosswalk,
      naturalEarthFeatureCollection,
      gdeltCountryLookupTsv: gdeltCountryLookupBuffer.toString("utf8"),
    });
    const artifact = await prepareCountryGeometryArtifact({
      naturalEarthFeatureCollection,
      validatedCrosswalk,
      context,
      profileId: profile.profileId,
    });
    const candidateDirectory = resolveCandidateDirectory({
      buildRoot: BUILD_ROOT,
      context,
      profileId: profile.profileId,
    });
    const writeResult = await writeCandidateArtifact({
      directoryPath: candidateDirectory,
      artifact,
      context,
    });
    const verification = await verifyArtifactPair({
      directoryPath: candidateDirectory,
      context,
    });
    return {
      command,
      profileId: profile.profileId,
      candidateDirectory,
      reused: writeResult.reused,
      sources: sources.map(({ sourceId, fileName, bytes, sha256, reused }) => ({
        sourceId,
        fileName,
        bytes,
        sha256,
        reused,
      })),
      metrics: verification.metrics,
    };
  }

  if (command === "verify") {
    const profile = selectCandidateProfile(
      context.geometryConfig,
      options.profileId,
    );
    const directoryPath = options.published
      ? join(STATIC_GEOMETRY_ROOT, context.geometryConfig.geometryVersion)
      : options.directoryPath ?? resolveCandidateDirectory({
        buildRoot: BUILD_ROOT,
        context,
        profileId: profile.profileId,
      });
    const verification = await verifyArtifactPair({
      directoryPath,
      context,
      requireSelectedProfile: options.published,
    });
    return {
      command,
      directoryPath: verification.directoryPath,
      profileId: verification.profile.profileId,
      metrics: verification.metrics,
    };
  }

  if (command === "publish") {
    const candidateDirectory = options.directoryPath ?? resolveCandidateDirectory({
      buildRoot: BUILD_ROOT,
      context,
      profileId: context.geometryConfig.selectedProfileId,
    });
    return {
      command,
      ...await publishGeometryArtifact({
        candidateDirectory,
        repositoryRoot: REPOSITORY_ROOT,
        context,
      }),
    };
  }

  throw new Error(`Неизвестная команда map geometry: ${String(command)}`);
}

export async function runCli(command, rawArguments = process.argv.slice(2)) {
  try {
    const summary = await runMapGeometryCommand(command, rawArguments);
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`Ошибка map geometry: ${message}\n`);
    process.exitCode = 1;
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

function parseCliArguments(command, rawArguments) {
  const options = {
    profileId: undefined,
    directoryPath: undefined,
    published: false,
  };
  for (let index = 0; index < rawArguments.length; index += 1) {
    const argument = rawArguments[index];
    if (argument === "--profile") {
      options.profileId = requireOptionValue(rawArguments, ++index, argument);
    } else if (argument === "--directory") {
      const value = requireOptionValue(rawArguments, ++index, argument);
      options.directoryPath = resolve(REPOSITORY_ROOT, value);
    } else if (argument === "--published" && command === "verify") {
      options.published = true;
    } else {
      throw new Error(`Неизвестный аргумент ${String(argument)} для команды ${command}`);
    }
  }
  if (options.published && options.directoryPath !== undefined) {
    throw new Error("Нельзя одновременно указывать --published и --directory");
  }
  if (command === "publish" && options.profileId !== undefined) {
    throw new Error("Publish всегда использует selectedProfileId из geometry config");
  }
  return options;
}

function requireOptionValue(argumentsList, index, optionName) {
  const value = argumentsList[index];
  if (typeof value !== "string" || value.length === 0 || value.startsWith("--")) {
    throw new Error(`Аргумент ${optionName} требует значение`);
  }
  return value;
}
