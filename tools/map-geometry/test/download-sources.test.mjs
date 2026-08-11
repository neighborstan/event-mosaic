import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

import {
  downloadSources,
  readVerifiedSourceBuffer,
  verifyDownloadedSources,
} from "../src/download-sources.mjs";
import { validateSourceManifest } from "../src/source-manifest.mjs";

test("Строгая схема source manifest отклоняет лишние поля, дубли и небезопасные значения", () => {
  const bytes = Buffer.from("fixture source", "utf8");

  assert.throws(
    () =>
      validateSourceManifest({
        ...manifestFor(bytes),
        unexpected: true,
      }),
    /должен содержать ровно поля/,
  );

  assert.throws(
    () =>
      validateSourceManifest(
        manifestFor(bytes, {
          fileName: "../source.txt",
        }),
      ),
    /безопасным basename/,
  );
  assert.throws(
    () =>
      validateSourceManifest(
        manifestFor(bytes, {
          url: "http:\/\/example.test\/source.txt",
        }),
      ),
    /HTTPS URL/,
  );
  assert.throws(
    () =>
      validateSourceManifest(
        manifestFor(bytes, {
          maxBytes: 0,
        }),
      ),
    /положительным целым числом/,
  );

  const duplicated = manifestFor(bytes);
  duplicated.sources.push({
    ...duplicated.sources[0],
    fileName: "another-source.txt",
  });
  assert.throws(
    () => validateSourceManifest(duplicated),
    /sourceId дублирует значение/,
  );
});

test("Поставляемый source manifest фиксирует оба проверяемых входа", async () => {
  const manifest = validateSourceManifest(
    JSON.parse(
      await readFile(
        new URL("../data/source-manifest.json", import.meta.url),
        "utf8",
      ),
    ),
  );

  assert.deepEqual(
    manifest.sources.map(({ sourceId, sha256 }) => ({ sourceId, sha256 })),
    [
      {
        sourceId: "natural-earth-10m-admin-0-countries",
        sha256:
          "239eec57ac17f100a11e2536cffc56752c318b50ae765b0918ff7aab4ce8f255",
      },
      {
        sourceId: "gdelt-country-lookup",
        sha256:
          "5c749d1e447b47be45de78702ab49e9697cc54a0bf058f13664cdbe5c71beec1",
      },
    ],
  );
});

test("При несовпадающем SHA-256 не оставляет итоговый файл или временную часть", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const expected = Buffer.from("expected bytes", "utf8");
  const received = Buffer.from("changed upstream bytes", "utf8");
  const manifest = manifestFor(expected);

  await assert.rejects(
    downloadSources({
      manifest,
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: fakeFetch(received),
    }),
    /SHA-256 загруженного source .* не совпадает/,
  );

  assert.deepEqual(await readdir(join(buildRoot, "inputs")), []);
});

test("Отклоняет redirect, неверный status, запрещенный тип и слишком большой Content-Length", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const bytes = Buffer.from("fixture source", "utf8");
  const manifest = manifestFor(bytes);
  const cases = [
    {
      name: "redirect",
      response: { redirected: true },
      expected: /вернул redirect/,
    },
    {
      name: "частичный HTTP-ответ",
      response: { status: 206 },
      expected: /HTTP status 206/,
    },
    {
      name: "запрещенный media type",
      response: { contentType: "text/html" },
      expected: /запрещенный Content-Type/,
    },
    {
      name: "слишком большой заголовок размера",
      response: { contentLength: 2048 },
      expected: /превышает maxBytes по Content-Length/,
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      await assert.rejects(
        downloadSources({
          manifest,
          repositoryRoot: repositoryRootFor(buildRoot),
          buildRoot,
          fetchImpl: fakeFetch(bytes, scenario.response),
        }),
        scenario.expected,
      );
      assert.deepEqual(await readdir(join(buildRoot, "inputs")), []);
    });
  }
});

test("Скачивает проверенные bytes один раз и повторно использует файл только после проверки", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const bytes = Buffer.from("verified fixture source", "utf8");
  const manifest = manifestFor(bytes);
  const fetchCalls = [];

  const first = await downloadSources({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
    fetchImpl: fakeFetch(bytes, {
      contentType: "text/plain; charset=utf-8",
      onCall(url, options) {
        fetchCalls.push({ url, options });
      },
    }),
  });

  assert.equal(first[0].reused, false);
  assert.deepEqual(
    await readFile(join(buildRoot, "inputs", "source.txt")),
    bytes,
  );
  assert.equal(fetchCalls.length, 1);
  assert.equal(fetchCalls[0].url, "https://example.test/source.txt");
  assert.equal(fetchCalls[0].options.redirect, "error");
  assert.equal(typeof fetchCalls[0].options.signal?.addEventListener, "function");
  assert.equal(fetchCalls[0].options.signal.aborted, false);

  const offline = await verifyDownloadedSources({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
  });
  assert.equal(offline[0].reused, true);

  let unexpectedFetch = false;
  const reused = await downloadSources({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
    fetchImpl: async () => {
      unexpectedFetch = true;
      throw new Error("fetch не должен вызываться");
    },
  });
  assert.equal(reused[0].reused, true);
  assert.equal(unexpectedFetch, false);

  await writeFile(join(buildRoot, "inputs", "source.txt"), "tampered", "utf8");
  await assert.rejects(
    downloadSources({
      manifest,
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: async () => {
        unexpectedFetch = true;
        throw new Error("fetch не должен вызываться");
      },
    }),
    /SHA-256 локального source .* не совпадает/,
  );
  assert.equal(unexpectedFetch, false);
});

test("Безопасное чтение повторно проверяет именно возвращаемый Buffer после подмены файла", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const bytes = Buffer.from("verified bytes for parser", "utf8");
  const manifest = manifestFor(bytes);

  await downloadSources({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
    fetchImpl: fakeFetch(bytes),
  });
  await verifyDownloadedSources({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
  });

  const verifiedBuffer = await readVerifiedSourceBuffer({
    manifest,
    repositoryRoot: repositoryRootFor(buildRoot),
    buildRoot,
    sourceId: "fixture-source",
  });
  assert.equal(Buffer.isBuffer(verifiedBuffer), true);
  assert.deepEqual(verifiedBuffer, bytes);

  const tampered = Buffer.from(bytes);
  tampered[0] ^= 0xff;
  await writeFile(join(buildRoot, "inputs", "source.txt"), tampered);

  await assert.rejects(
    readVerifiedSourceBuffer({
      manifest,
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      sourceId: "fixture-source",
    }),
    /SHA-256 локального source .* не совпадает/,
  );
});

test("Не публикует подмененный временный файл и сохраняет чужую замену", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const expected = Buffer.from("verified source bytes", "utf8");
  const replacement = Buffer.from("unverified replacement", "utf8");
  const inputsRoot = join(buildRoot, "inputs");
  let replacedPartName;

  await assert.rejects(
    downloadSources({
      manifest: manifestFor(expected),
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: async () => ({
        status: 200,
        redirected: false,
        headers: {
          get(name) {
            if (name.toLowerCase() === "content-type") {
              return "text/plain";
            }
            if (name.toLowerCase() === "content-length") {
              return String(expected.byteLength);
            }
            return null;
          },
        },
        body: {
          async *[Symbol.asyncIterator]() {
            yield expected;
            const partNames = (await readdir(inputsRoot)).filter((name) =>
              name.endsWith(".part"),
            );
            assert.equal(partNames.length, 1);
            [replacedPartName] = partNames;
            const partPath = join(inputsRoot, replacedPartName);
            await unlink(partPath);
            await writeFile(partPath, replacement);
          },
          async cancel() {},
        },
      }),
    }),
    (error) =>
      error instanceof Error &&
      error.message ===
        'Временный source "fixture-source" указывает на другой файл',
  );

  assert.notEqual(replacedPartName, undefined);
  assert.deepEqual(await readdir(inputsRoot), [replacedPartName]);
  assert.deepEqual(
    await readFile(join(inputsRoot, replacedPartName)),
    replacement,
  );
  await assert.rejects(readFile(join(inputsRoot, "source.txt")), {
    code: "ENOENT",
  });
});

test("Не публикует измененные bytes открытого временного файла", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const expected = Buffer.from("good-bytes", "utf8");
  const replacement = Buffer.from("evil-bytes", "utf8");
  const inputsRoot = join(buildRoot, "inputs");

  assert.equal(expected.byteLength, replacement.byteLength);
  await assert.rejects(
    downloadSources({
      manifest: manifestFor(expected),
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: async () => ({
        status: 200,
        redirected: false,
        headers: {
          get(name) {
            if (name.toLowerCase() === "content-type") {
              return "text/plain";
            }
            if (name.toLowerCase() === "content-length") {
              return String(expected.byteLength);
            }
            return null;
          },
        },
        body: {
          async *[Symbol.asyncIterator]() {
            yield expected;
            const [partName] = (await readdir(inputsRoot)).filter((name) =>
              name.endsWith(".part"),
            );
            assert.notEqual(partName, undefined);
            const partPath = join(inputsRoot, partName);
            const before = await lstat(partPath, { bigint: true });
            await writeFile(partPath, replacement);
            const after = await lstat(partPath, { bigint: true });
            assert.equal(after.dev, before.dev);
            assert.equal(after.ino, before.ino);
          },
          async cancel() {},
        },
      }),
    }),
    (error) =>
      error instanceof Error &&
      error.message ===
        'SHA-256 временного source "fixture-source" не совпадает с manifest',
  );

  assert.deepEqual(await readdir(inputsRoot), []);
  await assert.rejects(readFile(join(inputsRoot, "source.txt")), {
    code: "ENOENT",
  });
});

test("Останавливает поток при превышении maxBytes и удаляет только свою временную часть", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const bytes = Buffer.from("12345", "utf8");
  const manifest = manifestFor(bytes, { maxBytes: 4 });
  await mkdir(join(buildRoot, "inputs"), { recursive: true });
  await writeFile(
    join(buildRoot, "inputs", ".foreign-attempt.part"),
    "do not remove",
    "utf8",
  );

  await assert.rejects(
    downloadSources({
      manifest,
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: fakeFetch([bytes.subarray(0, 3), bytes.subarray(3)], {
        contentLength: null,
      }),
    }),
    /превысил maxBytes во время чтения/,
  );

  assert.deepEqual(await readdir(join(buildRoot, "inputs")), [
    ".foreign-attempt.part",
  ]);
});

test(
  "Общий deadline отменяет зависший поток и удаляет созданную временную часть",
  { timeout: 1000 },
  async (t) => {
    const buildRoot = await createTemporaryRoot(t);
    const expected = Buffer.from("complete fixture", "utf8");
    const partial = Buffer.from("partial", "utf8");
    const state = { cancelled: false, signal: null };

    await assert.rejects(
      downloadSources({
        manifest: manifestFor(expected),
        repositoryRoot: repositoryRootFor(buildRoot),
        buildRoot,
        fetchImpl: hangingFetch(partial, state),
        timeoutMs: 25,
      }),
      /превысил общий deadline 25 ms/,
    );

    assert.equal(state.signal.aborted, true);
    assert.equal(state.cancelled, true);
    assert.deepEqual(await readdir(join(buildRoot, "inputs")), []);
  },
);

test("Отклоняет symbolic link вместо каталога inputs", async (t) => {
  const buildRoot = await createTemporaryRoot(t);
  const workspaceRoot = resolve(buildRoot, "..", "..");
  const outsideRoot = join(workspaceRoot, "outside");
  await mkdir(buildRoot, { recursive: true });
  await mkdir(outsideRoot);

  try {
    await symlink(
      outsideRoot,
      join(buildRoot, "inputs"),
      process.platform === "win32" ? "junction" : "dir",
    );
  } catch (error) {
    if (error.code === "EPERM" || error.code === "EACCES") {
      t.skip("Среда не разрешает создать test-only symbolic link");
      return;
    }
    throw error;
  }

  let fetchCalled = false;
  await assert.rejects(
    downloadSources({
      manifest: manifestFor(Buffer.from("fixture", "utf8")),
      repositoryRoot: repositoryRootFor(buildRoot),
      buildRoot,
      fetchImpl: async () => {
        fetchCalled = true;
        throw new Error("fetch не должен вызываться");
      },
    }),
    /symbolic link запрещен/,
  );
  assert.equal(fetchCalled, false);
  assert.deepEqual(await readdir(outsideRoot), []);
});

test("Не идет по ссылке в пути build и не начинает загрузку", async (t) => {
  const workspaceRoot = await createTemporaryWorkspace(t);
  const buildRoot = join(workspaceRoot, "build", "map-geometry");
  const outsideRoot = join(workspaceRoot, "outside");
  await mkdir(outsideRoot);

  try {
    await symlink(
      outsideRoot,
      join(workspaceRoot, "build"),
      process.platform === "win32" ? "junction" : "dir",
    );
  } catch (error) {
    if (error.code === "EPERM" || error.code === "EACCES") {
      t.skip("Среда не разрешает создать test-only symbolic link");
      return;
    }
    throw error;
  }

  let fetchCalls = 0;
  await assert.rejects(
    downloadSources({
      manifest: manifestFor(Buffer.from("fixture", "utf8")),
      repositoryRoot: workspaceRoot,
      buildRoot,
      fetchImpl: async () => {
        fetchCalls += 1;
        throw new Error("fetch не должен вызываться");
      },
    }),
    (error) =>
      error instanceof Error &&
      error.message === "build отклонен: symbolic link запрещен",
  );
  assert.equal(fetchCalls, 0);
  assert.deepEqual(await readdir(outsideRoot), []);
  assert.equal(
    (await lstat(join(workspaceRoot, "build"))).isSymbolicLink(),
    true,
  );
});

test("После ожидания загрузки отклоняет замену подготовленного build", async (t) => {
  const workspaceRoot = await createTemporaryWorkspace(t);
  const buildRoot = join(workspaceRoot, "build", "map-geometry");
  const originalBuild = join(workspaceRoot, "original-build");
  const replacementInputs = join(buildRoot, "inputs");
  const expected = Buffer.from("fixture", "utf8");
  let fetchCalls = 0;

  await assert.rejects(
    downloadSources({
      manifest: manifestFor(expected),
      repositoryRoot: workspaceRoot,
      buildRoot,
      fetchImpl: async (...args) => {
        fetchCalls += 1;
        await rename(join(workspaceRoot, "build"), originalBuild);
        await mkdir(replacementInputs, { recursive: true });
        return fakeFetch(expected)(...args);
      },
    }),
    (error) =>
      error instanceof Error &&
      error.message === "build изменен после подготовки inputs",
  );

  assert.equal(fetchCalls, 1);
  assert.deepEqual(await readdir(replacementInputs), []);
  assert.deepEqual(
    await readdir(join(originalBuild, "map-geometry", "inputs")),
    [],
  );
  assert.equal(
    (await lstat(join(workspaceRoot, "build"))).isDirectory(),
    true,
  );
});

test("Отклоняет buildRoot вне явно заданного корня репозитория", async (t) => {
  const workspaceRoot = await createTemporaryWorkspace(t);
  const repositoryRoot = join(workspaceRoot, "repository");
  const outsideRoot = join(workspaceRoot, "outside");
  const buildRoot = join(outsideRoot, "build", "map-geometry");
  await mkdir(repositoryRoot);
  await mkdir(outsideRoot);

  let fetchCalls = 0;
  await assert.rejects(
    downloadSources({
      manifest: manifestFor(Buffer.from("fixture", "utf8")),
      repositoryRoot,
      buildRoot,
      fetchImpl: async () => {
        fetchCalls += 1;
        throw new Error("fetch не должен вызываться");
      },
    }),
    (error) =>
      error instanceof Error &&
      error.message ===
        "buildRoot должен указывать на repositoryRoot/build/map-geometry",
  );
  assert.equal(fetchCalls, 0);
  assert.deepEqual(await readdir(outsideRoot), []);
});

function manifestFor(bytes, overrides = {}) {
  const source = {
    sourceId: "fixture-source",
    fileName: "source.txt",
    url: "https://example.test/source.txt",
    providerIdentity: "fixture-provider-v1",
    repositoryCommit: null,
    license: {
      name: "Fixture License",
      url: "https://example.test/license",
    },
    sha256: sha256(bytes),
    maxBytes: 1024,
    contentTypes: ["text/plain"],
    ...overrides,
  };

  return {
    manifestSchemaVersion: "1",
    sources: [source],
  };
}

function fakeFetch(chunks, options = {}) {
  const normalizedChunks = Array.isArray(chunks) ? chunks : [chunks];
  const contentType = options.contentType ?? "text/plain";
  const contentLength =
    options.contentLength === undefined
      ? normalizedChunks.reduce((total, chunk) => total + chunk.byteLength, 0)
      : options.contentLength;

  return async (url, fetchOptions) => {
    options.onCall?.(url, fetchOptions);
    return {
      status: options.status ?? 200,
      redirected: options.redirected ?? false,
      headers: {
        get(name) {
          if (name.toLowerCase() === "content-type") {
            return contentType;
          }
          if (name.toLowerCase() === "content-length") {
            return contentLength === null ? null : String(contentLength);
          }
          return null;
        },
      },
      body: {
        async *[Symbol.asyncIterator]() {
          for (const chunk of normalizedChunks) {
            yield chunk;
          }
        },
        async cancel() {},
      },
    };
  };
}

function hangingFetch(partialChunk, state) {
  return async (_url, fetchOptions) => {
    state.signal = fetchOptions.signal;
    let rejectWait;
    let cancelled = false;
    const waitForever = new Promise((_resolve, reject) => {
      rejectWait = reject;
    });

    return {
      status: 200,
      redirected: false,
      headers: {
        get(name) {
          return name.toLowerCase() === "content-type" ? "text/plain" : null;
        },
      },
      body: {
        async *[Symbol.asyncIterator]() {
          yield partialChunk;
          await waitForever;
        },
        async cancel() {
          state.cancelled = true;
          if (!cancelled) {
            cancelled = true;
            rejectWait(fetchOptions.signal.reason);
          }
        },
      },
    };
  };
}

async function createTemporaryRoot(t) {
  const workspaceRoot = await createTemporaryWorkspace(t);
  return join(workspaceRoot, "build", "map-geometry");
}

function repositoryRootFor(buildRoot) {
  return resolve(buildRoot, "..", "..");
}

async function createTemporaryWorkspace(t) {
  const root = await mkdtemp(join(tmpdir(), "event-mosaic-map-geometry-"));
  t.after(async () => {
    await rm(root, { recursive: true, force: true });
  });
  return root;
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}
