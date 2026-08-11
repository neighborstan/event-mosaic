import { randomUUID } from "node:crypto";
import { lstat, mkdir, open, realpath, rename, rm, unlink } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

import { readArtifactPair, verifyArtifactPair } from "./geometry-artifact.mjs";

const STATIC_GEOMETRY_SEGMENTS = [
  "src",
  "main",
  "resources",
  "static",
  "map",
  "geometry",
];

export async function publishGeometryArtifact({
  candidateDirectory,
  repositoryRoot,
  context,
  renameDirectory = rename,
}) {
  const candidate = await verifyArtifactPair({
    directoryPath: candidateDirectory,
    context,
    requireSelectedProfile: true,
  });
  const staticGeometryRoot = await prepareStaticGeometryRoot(repositoryRoot);
  const geometryVersion = context.geometryConfig.geometryVersion;
  const targetDirectory = join(staticGeometryRoot, geometryVersion);

  const beforeLock = await inspectTarget({ targetDirectory, candidate, context });
  if (beforeLock === "IDENTICAL") {
    return publicationSummary(targetDirectory, candidate, true, false);
  }
  if (beforeLock !== "ABSENT") {
    throw new Error(
      `Versioned target ${geometryVersion} уже существует с partial или другими bytes`,
    );
  }

  const lockPath = join(staticGeometryRoot, `.${geometryVersion}.publish.lock`);
  const lock = await acquirePublicationLock(lockPath, targetDirectory, candidate, context);
  if (lock === null) {
    return publicationSummary(targetDirectory, candidate, true, true);
  }

  const temporaryDirectory = join(
    staticGeometryRoot,
    `.${geometryVersion}.publish-${randomUUID()}`,
  );
  let temporaryDirectoryIdentity = null;
  let recoveredUnknownOutcome = false;
  try {
    const afterLock = await inspectTarget({ targetDirectory, candidate, context });
    if (afterLock === "IDENTICAL") {
      return publicationSummary(targetDirectory, candidate, true, false);
    }
    if (afterLock !== "ABSENT") {
      throw new Error(
        `Versioned target ${geometryVersion} появился с partial или другими bytes`,
      );
    }

    await mkdir(temporaryDirectory, { recursive: false });
    temporaryDirectoryIdentity = await lstat(temporaryDirectory);
    await writeArtifactFiles(temporaryDirectory, candidate);
    await verifyArtifactPair({
      directoryPath: temporaryDirectory,
      context,
      requireSelectedProfile: true,
      checkDirectoryVersion: false,
    });

    try {
      await renameDirectory(temporaryDirectory, targetDirectory);
    } catch (error) {
      const afterError = await inspectTarget({ targetDirectory, candidate, context });
      if (afterError !== "IDENTICAL") {
        if (afterError === "ABSENT") {
          throw error;
        }
        throw new Error(
          `Publish получил неопределенный результат, а target ${geometryVersion} содержит partial или другие bytes`,
          { cause: error },
        );
      }
      recoveredUnknownOutcome = true;
    }

    const finalState = await inspectTarget({ targetDirectory, candidate, context });
    if (finalState !== "IDENTICAL") {
      throw new Error(
        `После directory rename target ${geometryVersion} не совпадает с проверенной парой`,
      );
    }
    return publicationSummary(
      targetDirectory,
      candidate,
      recoveredUnknownOutcome,
      recoveredUnknownOutcome,
    );
  } finally {
    try {
      await removeOwnedTemporaryDirectory(
        temporaryDirectory,
        staticGeometryRoot,
        temporaryDirectoryIdentity,
      );
    } finally {
      await releasePublicationLock(lockPath, lock);
    }
  }
}

async function prepareStaticGeometryRoot(repositoryRoot) {
  const resolvedRepositoryRoot = resolve(repositoryRoot);
  const repositoryStat = await lstat(resolvedRepositoryRoot);
  if (!repositoryStat.isDirectory() || repositoryStat.isSymbolicLink()) {
    throw new Error("Repository root должен быть обычным каталогом без ссылок");
  }
  if (resolve(await realpath(resolvedRepositoryRoot)) !== resolvedRepositoryRoot) {
    throw new Error("Repository root разрешается в неожиданный путь");
  }

  let current = resolvedRepositoryRoot;
  for (const segment of STATIC_GEOMETRY_SEGMENTS) {
    current = await ensureDirectChildDirectory(current, segment);
  }
  return current;
}

async function ensureDirectChildDirectory(parentDirectory, childName) {
  const childDirectory = join(parentDirectory, childName);
  try {
    await mkdir(childDirectory, { recursive: false });
  } catch (error) {
    if (error?.code !== "EEXIST") {
      throw error;
    }
  }
  const childStat = await lstat(childDirectory);
  if (!childStat.isDirectory() || childStat.isSymbolicLink()) {
    throw new Error(`Static geometry path содержит ссылку или не-каталог: ${childDirectory}`);
  }
  const actualChild = resolve(await realpath(childDirectory));
  if (actualChild !== resolve(childDirectory) || dirname(actualChild) !== parentDirectory) {
    throw new Error(`Static geometry path выходит за ожидаемого родителя: ${childDirectory}`);
  }
  return actualChild;
}

async function acquirePublicationLock(lockPath, targetDirectory, candidate, context) {
  let lockHandle;
  try {
    lockHandle = await open(lockPath, "wx", 0o600);
  } catch (error) {
    if (error?.code !== "EEXIST") {
      throw error;
    }
    const targetState = await inspectTarget({ targetDirectory, candidate, context });
    if (targetState === "IDENTICAL") {
      return null;
    }
    throw new Error(
      `Публикация ${context.geometryConfig.geometryVersion} уже удерживает exclusive lock`,
      { cause: error },
    );
  }

  let lockStat;
  try {
    lockStat = await lockHandle.stat();
    await lockHandle.writeFile(
      Buffer.from(`${context.geometryConfig.geometryVersion}:${randomUUID()}\n`, "utf8"),
    );
    await lockHandle.sync();
    return { handle: lockHandle, stat: lockStat };
  } catch (error) {
    try {
      await lockHandle.close();
    } finally {
      await removeOwnedPublicationLock(lockPath, lockStat);
    }
    throw error;
  }
}

async function releasePublicationLock(lockPath, lock) {
  try {
    await lock.handle.close();
  } finally {
    await removeOwnedPublicationLock(lockPath, lock.stat);
  }
}

async function removeOwnedPublicationLock(lockPath, expectedIdentity) {
  if (expectedIdentity === undefined) {
    return;
  }
  let pathStat;
  try {
    pathStat = await lstat(lockPath);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return;
    }
    throw error;
  }
  if (sameFileIdentity(expectedIdentity, pathStat)) {
    await unlink(lockPath);
  }
}

async function inspectTarget({ targetDirectory, candidate, context }) {
  let pair;
  try {
    pair = await readArtifactPair(targetDirectory, {
      maxGeojsonBytes: context.geometryConfig.budgets.maxGeojsonBytes,
    });
  } catch (error) {
    if (error?.code === "ENOENT") {
      return "ABSENT";
    }
    return "PARTIAL_OR_INVALID";
  }
  const identical =
    pair.countriesGeojsonBytes.equals(candidate.countriesGeojsonBytes) &&
    pair.manifestBytes.equals(candidate.manifestBytes);
  if (!identical) {
    return "DIFFERENT";
  }
  await verifyArtifactPair({
    directoryPath: targetDirectory,
    context,
    requireSelectedProfile: true,
  });
  return "IDENTICAL";
}

async function writeArtifactFiles(directoryPath, candidate) {
  await Promise.all([
    writeExclusiveFile(
      join(directoryPath, "countries.geojson"),
      candidate.countriesGeojsonBytes,
    ),
    writeExclusiveFile(
      join(directoryPath, "manifest.json"),
      candidate.manifestBytes,
    ),
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

async function removeOwnedTemporaryDirectory(
  temporaryDirectory,
  staticGeometryRoot,
  expectedIdentity,
) {
  if (
    dirname(temporaryDirectory) !== staticGeometryRoot ||
    !basename(temporaryDirectory).includes(".publish-")
  ) {
    throw new Error("Отказ очищать каталог, который не похож на собственный publish temp");
  }
  if (expectedIdentity === null) {
    return;
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
    throw new Error("Publish temp был заменен; чужой путь оставлен без удаления");
  }
  try {
    await rm(temporaryDirectory, { recursive: true, force: false });
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

function publicationSummary(targetDirectory, candidate, reused, recoveredUnknownOutcome) {
  return {
    directoryPath: targetDirectory,
    geometryVersion: candidate.manifest.geometryVersion,
    profileId: candidate.profile.profileId,
    reused,
    recoveredUnknownOutcome,
    metrics: candidate.metrics,
  };
}

function sameFileIdentity(expected, actual) {
  const sameInode = expected.ino !== 0 && expected.ino === actual.ino;
  const comparableDevice = expected.dev !== 0 && actual.dev !== 0;
  return sameInode && (!comparableDevice || expected.dev === actual.dev);
}
