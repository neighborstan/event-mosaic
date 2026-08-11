import assert from "node:assert/strict";
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { readArtifactPair } from "../src/geometry-artifact.mjs";
import { publishGeometryArtifact } from "../src/geometry-publisher.mjs";
import {
  createTestContext,
  deepClone,
  loadGeometryFixture,
  prepareTestArtifact,
} from "./geometry-test-fixtures.mjs";

const STATIC_GEOMETRY_SEGMENTS = [
  "src",
  "main",
  "resources",
  "static",
  "map",
  "geometry",
];

test("Публикация отклоняет валидный, но невыбранный profile до создания static path", async (t) => {
  const scenario = await createScenario(t);
  const fixture = await loadGeometryFixture();
  const nonSelectedArtifact = await prepareTestArtifact({
    fixture,
    context: scenario.context,
    profileId: "retain-80-p3",
  });
  const nonSelectedDirectory = join(
    scenario.temporaryRoot,
    "невыбранный кандидат",
    scenario.context.geometryConfig.geometryVersion,
  );
  await writeArtifactPair(nonSelectedDirectory, nonSelectedArtifact);

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: nonSelectedDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
    }),
    /Публиковать можно только selected profile retain-90-p3/,
  );

  assert.deepEqual(await readdir(scenario.repositoryRoot), []);
});

test("Публикация отклоняет поврежденный candidate до создания static path", async (t) => {
  const scenario = await createScenario(t);
  const countriesPath = join(scenario.candidateDirectory, "countries.geojson");
  const originalBytes = await readFile(countriesPath);
  await writeFile(countriesPath, Buffer.concat([originalBytes, Buffer.from("\n")]));

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
    }),
    /ожидается ровно один завершающий LF/,
  );

  assert.deepEqual(await readdir(scenario.repositoryRoot), []);
});

test("Публикация создает отсутствующую versioned пару в пути с пробелами и кириллицей", async (t) => {
  const scenario = await createScenario(t);

  const result = await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
  });

  assert.equal(result.directoryPath, scenario.targetDirectory);
  assert.equal(result.geometryVersion, "country-v1");
  assert.equal(result.profileId, "retain-90-p3");
  assert.equal(result.reused, false);
  assert.equal(result.recoveredUnknownOutcome, false);
  await assertTargetEqualsArtifact(scenario.targetDirectory, scenario.artifact);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Повторная публикация той же пары идемпотентно сохраняет прежние bytes", async (t) => {
  const scenario = await createScenario(t);
  await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
  });
  const before = await readArtifactPair(scenario.targetDirectory);
  const countriesPath = join(scenario.targetDirectory, "countries.geojson");
  const manifestPath = join(scenario.targetDirectory, "manifest.json");
  const countriesStatBefore = await stat(countriesPath);
  const manifestStatBefore = await stat(manifestPath);
  let renameCalled = false;

  const repeated = await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
    renameDirectory: async () => {
      renameCalled = true;
      throw new Error("Идентичный повтор не должен выполнять rename");
    },
  });
  const after = await readArtifactPair(scenario.targetDirectory);
  const countriesStatAfter = await stat(countriesPath);
  const manifestStatAfter = await stat(manifestPath);

  assert.equal(repeated.reused, true);
  assert.equal(repeated.recoveredUnknownOutcome, false);
  assert.equal(renameCalled, false);
  assert.deepEqual(after.countriesGeojsonBytes, before.countriesGeojsonBytes);
  assert.deepEqual(after.manifestBytes, before.manifestBytes);
  assert.equal(countriesStatAfter.ino, countriesStatBefore.ino);
  assert.equal(manifestStatAfter.ino, manifestStatBefore.ino);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Другая пара той же версии отклоняется и не меняет опубликованные bytes", async (t) => {
  const scenario = await createScenario(t, { includeDifferentCandidate: true });
  await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
  });
  const before = await readArtifactPair(scenario.targetDirectory);

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.differentCandidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
    }),
    /уже существует с partial или другими bytes/,
  );
  const after = await readArtifactPair(scenario.targetDirectory);

  assert.deepEqual(after.countriesGeojsonBytes, before.countriesGeojsonBytes);
  assert.deepEqual(after.manifestBytes, before.manifestBytes);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Частично существующая версия отклоняется без дополнения или перезаписи", async (t) => {
  const scenario = await createScenario(t);
  await mkdir(scenario.targetDirectory, { recursive: true });
  const countriesPath = join(scenario.targetDirectory, "countries.geojson");
  await writeFile(countriesPath, scenario.artifact.countriesGeojsonBytes);
  const before = await readFile(countriesPath);

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
    }),
    /уже существует с partial или другими bytes/,
  );

  assert.deepEqual(await readFile(countriesPath), before);
  assert.deepEqual(await readdir(scenario.targetDirectory), ["countries.geojson"]);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Пустой существующий versioned каталог тоже считается immutable target", async (t) => {
  const scenario = await createScenario(t);
  await mkdir(scenario.targetDirectory, { recursive: true });

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
    }),
    /уже существует с partial или другими bytes/,
  );

  assert.deepEqual(await readdir(scenario.targetDirectory), []);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Успешный rename с последующей ошибкой распознается как завершенная публикация", async (t) => {
  const scenario = await createScenario(t);
  const renameThenThrow = async (source, target) => {
    await rename(source, target);
    throw new Error("Имитированная потеря результата rename");
  };

  const result = await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
    renameDirectory: renameThenThrow,
  });

  assert.equal(result.reused, true);
  assert.equal(result.recoveredUnknownOutcome, true);
  await assertTargetEqualsArtifact(scenario.targetDirectory, scenario.artifact);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Ошибка rename до перемещения не оставляет собственный temp или lock", async (t) => {
  const scenario = await createScenario(t);

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
      renameDirectory: async () => {
        throw new Error("Имитированная ошибка до rename");
      },
    }),
    /Имитированная ошибка до rename/,
  );

  await assert.rejects(stat(scenario.targetDirectory), { code: "ENOENT" });
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Неопределенный rename с partial target завершается ошибкой без дополнения", async (t) => {
  const scenario = await createScenario(t);
  const createPartialThenThrow = async (source, target) => {
    await mkdir(target, { recursive: false });
    await rename(
      join(source, "countries.geojson"),
      join(target, "countries.geojson"),
    );
    throw new Error("Имитированный partial rename");
  };

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
      renameDirectory: createPartialThenThrow,
    }),
    /неопределенный результат.*partial или другие bytes/,
  );

  assert.deepEqual(await readdir(scenario.targetDirectory), ["countries.geojson"]);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Очистка удаляет только собственные temp и lock, сохраняя чужой каталог", async (t) => {
  const scenario = await createScenario(t);
  const foreignDirectory = join(
    scenario.staticGeometryRoot,
    ".country-v1.publish-foreign",
  );
  const sentinelPath = join(foreignDirectory, "не трогать.txt");
  await mkdir(foreignDirectory, { recursive: true });
  await writeFile(sentinelPath, "foreign\n", "utf8");

  await publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
  });

  assert.equal(await readFile(sentinelPath, "utf8"), "foreign\n");
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot, {
    preservedNames: [".country-v1.publish-foreign"],
  });
});

test("Замененный publish temp не удаляется, а exclusive lock освобождается", async (t) => {
  const scenario = await createScenario(t);
  let replacementPath;

  await assert.rejects(
    publishGeometryArtifact({
      candidateDirectory: scenario.candidateDirectory,
      repositoryRoot: scenario.repositoryRoot,
      context: scenario.context,
      renameDirectory: async (source) => {
        replacementPath = source;
        await rm(source, { recursive: true, force: false });
        await mkdir(source, { recursive: false });
        await writeFile(join(source, "чужой файл.txt"), "foreign\n", "utf8");
        throw new Error("Temp был заменен до завершения rename");
      },
    }),
    /Publish temp был заменен; чужой путь оставлен без удаления/,
  );

  assert.equal(
    await readFile(join(replacementPath, "чужой файл.txt"), "utf8"),
    "foreign\n",
  );
  const entries = await readdir(scenario.staticGeometryRoot);
  assert.equal(entries.includes(".country-v1.publish.lock"), false);
});

test("Параллельная публикация той же пары не обходит exclusive lock", async (t) => {
  const scenario = await createScenario(t);
  const gate = createRenameGate();
  const firstPublication = publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
    renameDirectory: gate.renameDirectory,
  });
  await gate.entered;

  try {
    await assert.rejects(
      publishGeometryArtifact({
        candidateDirectory: scenario.candidateDirectory,
        repositoryRoot: scenario.repositoryRoot,
        context: scenario.context,
      }),
      /уже удерживает exclusive lock/,
    );
  } finally {
    gate.release();
  }

  await firstPublication;
  await assertTargetEqualsArtifact(scenario.targetDirectory, scenario.artifact);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

test("Параллельная публикация другой пары не заменяет выбранного победителя", async (t) => {
  const scenario = await createScenario(t, { includeDifferentCandidate: true });
  const gate = createRenameGate();
  const firstPublication = publishGeometryArtifact({
    candidateDirectory: scenario.candidateDirectory,
    repositoryRoot: scenario.repositoryRoot,
    context: scenario.context,
    renameDirectory: gate.renameDirectory,
  });
  await gate.entered;

  try {
    await assert.rejects(
      publishGeometryArtifact({
        candidateDirectory: scenario.differentCandidateDirectory,
        repositoryRoot: scenario.repositoryRoot,
        context: scenario.context,
      }),
      /уже удерживает exclusive lock/,
    );
  } finally {
    gate.release();
  }

  await firstPublication;
  await assertTargetEqualsArtifact(scenario.targetDirectory, scenario.artifact);
  await assertNoOwnedPublisherArtifacts(scenario.staticGeometryRoot);
});

async function createScenario(t, { includeDifferentCandidate = false } = {}) {
  const temporaryRoot = await mkdtemp(
    join(tmpdir(), "event-mosaic-publisher-путь с пробелами-"),
  );
  t.after(() => rm(temporaryRoot, { recursive: true, force: true }));
  const repositoryRoot = join(temporaryRoot, "репозиторий с пробелами");
  await mkdir(repositoryRoot, { recursive: true });

  const fixture = await loadGeometryFixture();
  const context = createTestContext();
  const artifact = await prepareTestArtifact({ fixture, context });
  const candidateDirectory = join(
    temporaryRoot,
    "проверенный кандидат",
    context.geometryConfig.geometryVersion,
  );
  await writeArtifactPair(candidateDirectory, artifact);

  let differentArtifact;
  let differentCandidateDirectory;
  if (includeDifferentCandidate) {
    const changedFixture = deepClone(fixture);
    const betaFeature = changedFixture.naturalEarthFeatureCollection.features.find(
      (feature) => String(feature.properties.NE_ID) === "3",
    );
    for (const position of betaFeature.geometry.coordinates[0]) {
      position[0] += 0.25;
    }
    differentArtifact = await prepareTestArtifact({
      fixture: changedFixture,
      context,
    });
    assert.notDeepEqual(
      differentArtifact.countriesGeojsonBytes,
      artifact.countriesGeojsonBytes,
    );
    differentCandidateDirectory = join(
      temporaryRoot,
      "другой кандидат",
      context.geometryConfig.geometryVersion,
    );
    await writeArtifactPair(differentCandidateDirectory, differentArtifact);
  }

  const staticGeometryRoot = join(repositoryRoot, ...STATIC_GEOMETRY_SEGMENTS);
  const targetDirectory = join(
    staticGeometryRoot,
    context.geometryConfig.geometryVersion,
  );
  return {
    temporaryRoot,
    repositoryRoot,
    context,
    artifact,
    candidateDirectory,
    differentArtifact,
    differentCandidateDirectory,
    staticGeometryRoot,
    targetDirectory,
  };
}

async function writeArtifactPair(directoryPath, artifact) {
  await mkdir(directoryPath, { recursive: true });
  await Promise.all([
    writeFile(join(directoryPath, "countries.geojson"), artifact.countriesGeojsonBytes),
    writeFile(join(directoryPath, "manifest.json"), artifact.manifestBytes),
  ]);
}

async function assertTargetEqualsArtifact(targetDirectory, artifact) {
  const published = await readArtifactPair(targetDirectory);
  assert.deepEqual(
    published.countriesGeojsonBytes,
    artifact.countriesGeojsonBytes,
  );
  assert.deepEqual(published.manifestBytes, artifact.manifestBytes);
}

async function assertNoOwnedPublisherArtifacts(
  staticGeometryRoot,
  { preservedNames = [] } = {},
) {
  const entries = await readdir(staticGeometryRoot);
  assert.equal(entries.includes(".country-v1.publish.lock"), false);
  assert.deepEqual(
    entries
      .filter((entry) => entry.startsWith(".country-v1.publish-"))
      .sort(),
    [...preservedNames].sort(),
  );
}

function createRenameGate() {
  let announceEntered;
  let releaseRename;
  const entered = new Promise((resolve) => {
    announceEntered = resolve;
  });
  const released = new Promise((resolve) => {
    releaseRename = resolve;
  });
  return {
    entered,
    release: releaseRename,
    renameDirectory: async (source, target) => {
      announceEntered();
      await released;
      await rename(source, target);
    },
  };
}
