import { constants } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import {
  link,
  lstat,
  mkdir,
  open,
  realpath,
  unlink,
} from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";

import { validateSourceManifest } from "./source-manifest.mjs";

const READ_BUFFER_BYTES = 64 * 1024;
const NO_FOLLOW = constants.O_NOFOLLOW ?? 0;
const MAX_TIMEOUT_MS = 2_147_483_647;
const BUILD_DIRECTORY_NAME = "build";
const TOOL_BUILD_DIRECTORY_NAME = "map-geometry";
const INPUTS_DIRECTORY_NAME = "inputs";

export const DEFAULT_SOURCE_TIMEOUT_MS = 120_000;

export async function downloadSources({
  manifest,
  repositoryRoot,
  buildRoot,
  fetchImpl = globalThis.fetch,
  timeoutMs = DEFAULT_SOURCE_TIMEOUT_MS,
} = {}) {
  const validatedManifest = validateSourceManifest(manifest);
  validateTimeoutMs(timeoutMs);
  const inputsContext = await prepareInputsRoot(repositoryRoot, buildRoot);
  const { inputsRoot } = inputsContext;
  const results = [];

  for (const source of validatedManifest.sources) {
    await assertSafeInputsRoot(inputsContext);
    const finalPath = join(inputsRoot, source.fileName);
    const existing = await inspectExistingSource(finalPath, source);
    if (existing !== null) {
      await assertSafeInputsRoot(inputsContext);
      results.push(existing);
      continue;
    }

    if (typeof fetchImpl !== "function") {
      throw new TypeError(
        `Для загрузки source \"${source.sourceId}\" требуется fetchImpl`,
      );
    }
    results.push(
      await downloadSource({
        source,
        inputsContext,
        finalPath,
        fetchImpl,
        timeoutMs,
      }),
    );
  }

  return Object.freeze(results);
}

export async function verifyDownloadedSources({
  manifest,
  repositoryRoot,
  buildRoot,
} = {}) {
  const validatedManifest = validateSourceManifest(manifest);
  const inputsContext = await prepareInputsRoot(repositoryRoot, buildRoot);
  const { inputsRoot } = inputsContext;
  const results = [];

  for (const source of validatedManifest.sources) {
    await assertSafeInputsRoot(inputsContext);
    const finalPath = join(inputsRoot, source.fileName);
    const existing = await inspectExistingSource(finalPath, source);
    if (existing === null) {
      throw new Error(
        `Проверяемый source \"${source.sourceId}\" отсутствует в inputs`,
      );
    }
    await assertSafeInputsRoot(inputsContext);
    results.push(existing);
  }

  return Object.freeze(results);
}

export async function readVerifiedSourceBuffer({
  manifest,
  repositoryRoot,
  buildRoot,
  sourceId,
} = {}) {
  const validatedManifest = validateSourceManifest(manifest);
  if (typeof sourceId !== "string" || sourceId.length === 0) {
    throw new TypeError("sourceId должен быть непустой строкой");
  }

  const source = validatedManifest.sources.find(
    (candidate) => candidate.sourceId === sourceId,
  );
  if (source === undefined) {
    throw new Error(`Source \"${sourceId}\" отсутствует в manifest`);
  }

  const inputsContext = await prepareInputsRoot(repositoryRoot, buildRoot);
  const { inputsRoot } = inputsContext;
  await assertSafeInputsRoot(inputsContext);
  const finalPath = join(inputsRoot, source.fileName);
  const pathStat = await inspectSourcePath(finalPath, source);
  if (pathStat === null) {
    throw new Error(`Проверяемый source \"${sourceId}\" отсутствует в inputs`);
  }

  const verified = await readBoundedLocalBuffer(finalPath, source, pathStat);
  await assertSafeInputsRoot(inputsContext);
  if (verified.sha256 !== source.sha256) {
    throw new Error(
      `SHA-256 локального source \"${source.sourceId}\" не совпадает с manifest`,
    );
  }
  return verified.buffer;
}

async function downloadSource({
  source,
  inputsContext,
  finalPath,
  fetchImpl,
  timeoutMs,
}) {
  const { inputsRoot } = inputsContext;
  const deadline = createDownloadDeadline(source, timeoutMs);
  let response;
  let fetchCompleted = false;
  let removeAbortListener = () => {};
  try {
    response = await fetchImpl(source.url, {
      redirect: "error",
      signal: deadline.signal,
    });
    fetchCompleted = true;
    removeAbortListener = cancelBodyOnAbort(response, deadline.signal);
    validateResponseStatus(response, source);
    validateResponseHeaders(response, source);
    if (
      response.body === null ||
      response.body === undefined ||
      typeof response.body[Symbol.asyncIterator] !== "function"
    ) {
      throw new Error(
        `Source \"${source.sourceId}\" не содержит потокового response body`,
      );
    }

    await assertSafeInputsRoot(inputsContext);
    const tempPath = join(
      inputsRoot,
      `.${source.sourceId}-${randomUUID()}.part`,
    );
    let tempOwned = false;
    let tempFileHandle = null;
    let ownedFileStat = null;

    try {
      const flags =
        constants.O_RDWR | constants.O_CREAT | constants.O_EXCL | NO_FOLLOW;
      tempFileHandle = await open(tempPath, flags, 0o600);
      tempOwned = true;
      const openedFileStat = await tempFileHandle.stat({ bigint: true });
      assertOwnedFileIdentity(openedFileStat, source);
      ownedFileStat = openedFileStat;
      await assertSafeInputsRoot(inputsContext);
      await assertOwnedPartPath(tempPath, source, ownedFileStat);
      const downloaded = await streamToOwnedPart({
        body: response.body,
        fileHandle: tempFileHandle,
        source,
      });
      const completedFileStat = await tempFileHandle.stat({ bigint: true });
      assertSameFileIdentity(
        openedFileStat,
        completedFileStat,
        `Временный source \"${source.sourceId}\" указывает на другой файл`,
      );
      if (
        !completedFileStat.isFile() ||
        completedFileStat.size !== BigInt(downloaded.bytes)
      ) {
        throw new Error(
          `Временный source \"${source.sourceId}\" изменился во время записи`,
        );
      }
      ownedFileStat = completedFileStat;
      deadline.throwIfExpired();
      deadline.complete();
      if (downloaded.sha256 !== source.sha256) {
        throw new Error(
          `SHA-256 загруженного source \"${source.sourceId}\" не совпадает с manifest`,
        );
      }

      const verifiedTemp = await hashOpenedFile(
        tempFileHandle,
        source,
        ownedFileStat,
        "Временный source",
      );
      if (verifiedTemp.sha256 !== source.sha256) {
        throw new Error(
          `SHA-256 временного source \"${source.sourceId}\" не совпадает с manifest`,
        );
      }
      ownedFileStat = verifiedTemp.fileStat;

      await assertSafeInputsRoot(inputsContext);
      await assertOwnedPartPath(tempPath, source, ownedFileStat);
      try {
        await link(tempPath, finalPath);
      } catch (error) {
        if (error.code !== "EEXIST") {
          throw error;
        }

        const concurrent = await inspectExistingSource(finalPath, source);
        if (concurrent === null) {
          throw new Error(
            `Source \"${source.sourceId}\" появился конкурентно, но недоступен для проверки`,
            { cause: error },
          );
        }
        await assertSafeInputsRoot(inputsContext);
        return concurrent;
      }

      await assertSafeInputsRoot(inputsContext);
      const published = await inspectLinkedSource(
        finalPath,
        source,
        ownedFileStat,
      );
      await assertSafeInputsRoot(inputsContext);
      return Object.freeze({
        sourceId: source.sourceId,
        fileName: source.fileName,
        filePath: finalPath,
        sha256: published.sha256,
        bytes: published.bytes,
        reused: false,
      });
    } finally {
      try {
        if (tempOwned && ownedFileStat !== null) {
          await removeOwnedPath(tempPath, ownedFileStat);
        }
      } finally {
        if (tempFileHandle !== null) {
          await tempFileHandle.close();
        }
      }
    }
  } catch (error) {
    await cancelResponseBody(response);
    if (deadline.expired) {
      throw new Error(
        `Source \"${source.sourceId}\" превысил общий deadline ${timeoutMs} ms`,
        { cause: error },
      );
    }
    if (!fetchCompleted) {
      throw new Error(`Не удалось загрузить source \"${source.sourceId}\"`, {
        cause: error,
      });
    }
    throw error;
  } finally {
    removeAbortListener();
    deadline.complete();
  }
}

async function streamToOwnedPart({ body, fileHandle, source }) {
  const hash = createHash("sha256");
  let bytes = 0;

  for await (const chunk of body) {
    if (!(chunk instanceof Uint8Array)) {
      throw new Error(
        `Source \"${source.sourceId}\" вернул неподдерживаемый тип chunk`,
      );
    }

    const buffer = Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    if (bytes + buffer.byteLength > source.maxBytes) {
      throw new Error(
        `Source \"${source.sourceId}\" превысил maxBytes во время чтения`,
      );
    }

    await writeAll(fileHandle, buffer);
    hash.update(buffer);
    bytes += buffer.byteLength;
  }

  await fileHandle.sync();
  return { bytes, sha256: hash.digest("hex") };
}

async function writeAll(fileHandle, buffer) {
  let offset = 0;
  while (offset < buffer.byteLength) {
    const { bytesWritten } = await fileHandle.write(
      buffer,
      offset,
      buffer.byteLength - offset,
      null,
    );
    if (bytesWritten <= 0) {
      throw new Error("Не удалось продолжить запись source input");
    }
    offset += bytesWritten;
  }
}

async function inspectExistingSource(finalPath, source) {
  const pathStat = await inspectSourcePath(finalPath, source);
  if (pathStat === null) {
    return null;
  }

  const verified = await hashLocalFile(finalPath, source, pathStat);
  if (verified.sha256 !== source.sha256) {
    throw new Error(
      `SHA-256 локального source \"${source.sourceId}\" не совпадает с manifest`,
    );
  }
  await assertSourcePathSnapshot(
    finalPath,
    source,
    verified.fileStat,
    `Локальный source \"${source.sourceId}\" изменился после проверки`,
  );

  return Object.freeze({
    sourceId: source.sourceId,
    fileName: source.fileName,
    filePath: finalPath,
    sha256: verified.sha256,
    bytes: verified.bytes,
    reused: true,
  });
}

async function inspectLinkedSource(finalPath, source, ownedFileStat) {
  const pathStat = await inspectSourcePath(finalPath, source);
  if (pathStat === null) {
    throw new Error(
      `Source \"${source.sourceId}\" исчез после create-only переноса`,
    );
  }
  assertSameFileIdentity(
    ownedFileStat,
    pathStat,
    `Итоговый source \"${source.sourceId}\" указывает на другой файл`,
  );

  const verified = await hashLocalFile(finalPath, source, pathStat);
  if (verified.sha256 !== source.sha256) {
    throw new Error(
      `SHA-256 итогового source \"${source.sourceId}\" не совпадает с manifest`,
    );
  }
  await assertSourcePathSnapshot(
    finalPath,
    source,
    verified.fileStat,
    `Итоговый source \"${source.sourceId}\" изменился после проверки`,
  );
  return verified;
}

async function assertSourcePathSnapshot(
  filePath,
  source,
  expectedFileStat,
  message,
) {
  const currentPathStat = await inspectSourcePath(filePath, source);
  if (currentPathStat === null) {
    throw new Error(message);
  }
  assertSameFileSnapshot(expectedFileStat, currentPathStat, message);
}

async function assertOwnedPartPath(tempPath, source, ownedFileStat) {
  let pathStat;
  try {
    pathStat = await lstat(tempPath, { bigint: true });
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error(
        `Временный source \"${source.sourceId}\" исчез перед create-only переносом`,
        { cause: error },
      );
    }
    throw error;
  }

  if (!pathStat.isFile()) {
    throw new Error(
      `Временный source \"${source.sourceId}\" больше не является обычным файлом`,
    );
  }
  assertSameFileIdentity(
    ownedFileStat,
    pathStat,
    `Временный source \"${source.sourceId}\" указывает на другой файл`,
  );
}

async function inspectSourcePath(finalPath, source) {
  let pathStat;
  try {
    pathStat = await lstat(finalPath, { bigint: true });
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }
    throw error;
  }

  if (pathStat.isSymbolicLink()) {
    throw new Error(
      `Source \"${source.sourceId}\" отклонен: final path является symbolic link`,
    );
  }
  if (!pathStat.isFile()) {
    throw new Error(
      `Source \"${source.sourceId}\" отклонен: final path не является файлом`,
    );
  }
  return pathStat;
}

async function readBoundedLocalBuffer(filePath, source, expectedPathStat) {
  const fileHandle = await open(filePath, constants.O_RDONLY | NO_FOLLOW);
  const readBuffer = Buffer.allocUnsafe(READ_BUFFER_BYTES);
  const chunks = [];
  let bytes = 0;

  try {
    const before = await fileHandle.stat({ bigint: true });
    if (!before.isFile()) {
      throw new Error(`Source \"${source.sourceId}\" не является обычным файлом`);
    }
    assertSameOpenedFile(expectedPathStat, before, source);
    if (before.size > BigInt(source.maxBytes)) {
      throw new Error(
        `Локальный source \"${source.sourceId}\" превышает maxBytes`,
      );
    }

    while (true) {
      const { bytesRead } = await fileHandle.read(
        readBuffer,
        0,
        readBuffer.byteLength,
        bytes,
      );
      if (bytesRead === 0) {
        break;
      }
      bytes += bytesRead;
      if (bytes > source.maxBytes) {
        throw new Error(
          `Локальный source \"${source.sourceId}\" превысил maxBytes во время чтения`,
        );
      }
      chunks.push(Buffer.from(readBuffer.subarray(0, bytesRead)));
    }

    const after = await fileHandle.stat({ bigint: true });
    if (
      !isSameFileIdentity(before, after) ||
      before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs ||
      after.size !== BigInt(bytes)
    ) {
      throw new Error(
        `Локальный source \"${source.sourceId}\" изменился во время чтения`,
      );
    }
  } finally {
    await fileHandle.close();
  }

  const buffer = Buffer.concat(chunks, bytes);
  return {
    buffer,
    sha256: createHash("sha256").update(buffer).digest("hex"),
  };
}

async function hashLocalFile(filePath, source, expectedPathStat) {
  const fileHandle = await open(filePath, constants.O_RDONLY | NO_FOLLOW);
  try {
    return await hashOpenedFile(
      fileHandle,
      source,
      expectedPathStat,
      "Локальный source",
    );
  } finally {
    await fileHandle.close();
  }
}

async function hashOpenedFile(
  fileHandle,
  source,
  expectedFileStat,
  sourceLabel,
) {
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(READ_BUFFER_BYTES);
  let bytes = 0;

  const before = await fileHandle.stat({ bigint: true });
  if (!before.isFile()) {
    throw new Error(
      `${sourceLabel} \"${source.sourceId}\" не является обычным файлом`,
    );
  }
  assertSameFileSnapshot(
    expectedFileStat,
    before,
    `${sourceLabel} \"${source.sourceId}\" изменился перед проверкой`,
  );
  if (before.size > BigInt(source.maxBytes)) {
    throw new Error(`${sourceLabel} \"${source.sourceId}\" превышает maxBytes`);
  }

  while (true) {
    const { bytesRead } = await fileHandle.read(
      buffer,
      0,
      buffer.byteLength,
      bytes,
    );
    if (bytesRead === 0) {
      break;
    }
    bytes += bytesRead;
    if (bytes > source.maxBytes) {
      throw new Error(
        `${sourceLabel} \"${source.sourceId}\" превысил maxBytes во время проверки`,
      );
    }
    hash.update(buffer.subarray(0, bytesRead));
  }

  const after = await fileHandle.stat({ bigint: true });
  if (
    !isSameFileIdentity(before, after) ||
    before.size !== after.size ||
    before.mtimeNs !== after.mtimeNs ||
    after.size !== BigInt(bytes)
  ) {
    throw new Error(
      `${sourceLabel} \"${source.sourceId}\" изменился во время проверки`,
    );
  }

  return { bytes, sha256: hash.digest("hex"), fileStat: after };
}

function assertSameOpenedFile(pathStat, handleStat, source) {
  assertSameFileSnapshot(
    pathStat,
    handleStat,
    `Локальный source \"${source.sourceId}\" изменился перед открытием`,
  );
}

function assertOwnedFileIdentity(fileStat, source) {
  if (!fileStat.isFile()) {
    throw new Error(
      `Временный source \"${source.sourceId}\" не является обычным файлом`,
    );
  }
  if (!hasStableFileIdentity(fileStat)) {
    throw new Error(
      `Файловая система не предоставляет стабильный идентификатор для source \"${source.sourceId}\"`,
    );
  }
}

function assertSameFileIdentity(expectedStat, actualStat, message) {
  if (!isSameFileIdentity(expectedStat, actualStat)) {
    throw new Error(message);
  }
}

function assertSameFileSnapshot(expectedStat, actualStat, message) {
  if (
    !isSameFileIdentity(expectedStat, actualStat) ||
    expectedStat.size !== actualStat.size ||
    expectedStat.mtimeNs !== actualStat.mtimeNs
  ) {
    throw new Error(message);
  }
}

function isSameFileIdentity(expectedStat, actualStat) {
  if (
    !hasStableFileIdentity(expectedStat) ||
    !hasStableFileIdentity(actualStat) ||
    expectedStat.ino !== actualStat.ino
  ) {
    return false;
  }

  // Windows может сообщить dev=0 через lstat и ненулевой dev через fstat для
  // того же inode. Сравниваем dev только когда обе стороны его предоставили.
  return (
    expectedStat.dev === 0n ||
    actualStat.dev === 0n ||
    expectedStat.dev === actualStat.dev
  );
}

function hasStableFileIdentity(fileStat) {
  // На Windows dev может быть 0, но ненулевой ino остается file identity.
  return fileStat.ino !== 0n;
}

function validateResponseStatus(response, source) {
  if (response === null || typeof response !== "object") {
    throw new Error(`Source \"${source.sourceId}\" вернул некорректный response`);
  }
  if (response.redirected === true) {
    throw new Error(`Source \"${source.sourceId}\" вернул redirect`);
  }
  if (response.status !== 200) {
    throw new Error(
      `Source \"${source.sourceId}\" вернул HTTP status ${String(response.status)}`,
    );
  }
}

function validateResponseHeaders(response, source) {
  if (response.headers === null || typeof response.headers?.get !== "function") {
    throw new Error(`Source \"${source.sourceId}\" не содержит HTTP headers`);
  }

  const rawContentType = response.headers.get("content-type");
  if (typeof rawContentType !== "string" || rawContentType.trim() === "") {
    throw new Error(`Source \"${source.sourceId}\" не содержит Content-Type`);
  }
  const mediaType = rawContentType.split(";", 1)[0].trim().toLowerCase();
  if (!source.contentTypes.includes(mediaType)) {
    throw new Error(
      `Source \"${source.sourceId}\" вернул запрещенный Content-Type \"${mediaType}\"`,
    );
  }

  const rawLength = response.headers.get("content-length");
  if (rawLength === null) {
    return;
  }
  if (!/^[0-9]+$/.test(rawLength)) {
    throw new Error(
      `Source \"${source.sourceId}\" вернул некорректный Content-Length`,
    );
  }

  const length = BigInt(rawLength);
  if (length > BigInt(source.maxBytes)) {
    throw new Error(
      `Source \"${source.sourceId}\" превышает maxBytes по Content-Length`,
    );
  }
}

function validateTimeoutMs(timeoutMs) {
  if (
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs <= 0 ||
    timeoutMs > MAX_TIMEOUT_MS
  ) {
    throw new TypeError(
      `timeoutMs должен быть целым числом от 1 до ${MAX_TIMEOUT_MS}`,
    );
  }
}

function createDownloadDeadline(source, timeoutMs) {
  const controller = new AbortController();
  let expired = false;
  let timer = setTimeout(() => {
    expired = true;
    controller.abort(
      new Error(
        `Source \"${source.sourceId}\" превысил общий deadline ${timeoutMs} ms`,
      ),
    );
  }, timeoutMs);

  return {
    signal: controller.signal,
    get expired() {
      return expired;
    },
    throwIfExpired() {
      if (expired) {
        throw controller.signal.reason;
      }
    },
    complete() {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

function cancelBodyOnAbort(response, signal) {
  const abortListener = () => {
    void cancelResponseBody(response);
  };
  signal.addEventListener("abort", abortListener, { once: true });
  return () => signal.removeEventListener("abort", abortListener);
}

async function prepareInputsRoot(repositoryRoot, buildRoot) {
  validateAbsolutePath(repositoryRoot, "repositoryRoot");
  validateAbsolutePath(buildRoot, "buildRoot");

  const requestedRepositoryRoot = resolve(repositoryRoot);
  const requestedRoot = resolve(buildRoot);
  const expectedRequestedRoot = join(
    requestedRepositoryRoot,
    BUILD_DIRECTORY_NAME,
    TOOL_BUILD_DIRECTORY_NAME,
  );
  assertSameResolvedPath(
    expectedRequestedRoot,
    requestedRoot,
    `buildRoot должен указывать на repositoryRoot/${BUILD_DIRECTORY_NAME}/${TOOL_BUILD_DIRECTORY_NAME}`,
  );
  assertOutsidePublishedResources(requestedRoot);

  const canonicalRepositoryRoot = await realpath(requestedRepositoryRoot);
  await assertDirectoryWithoutSymlink(
    canonicalRepositoryRoot,
    "repositoryRoot",
  );
  assertOutsidePublishedResources(canonicalRepositoryRoot);

  const buildDirectory = await ensureDirectChildDirectory(
    canonicalRepositoryRoot,
    BUILD_DIRECTORY_NAME,
    "build",
  );
  const canonicalRoot = await ensureDirectChildDirectory(
    buildDirectory,
    TOOL_BUILD_DIRECTORY_NAME,
    "buildRoot",
  );
  assertOutsidePublishedResources(canonicalRoot);
  const inputsRoot = await ensureDirectChildDirectory(
    canonicalRoot,
    INPUTS_DIRECTORY_NAME,
    "inputs",
  );
  const inputsContext = Object.freeze({
    repositoryRoot: canonicalRepositoryRoot,
    repositoryStat: await assertDirectoryWithoutSymlink(
      canonicalRepositoryRoot,
      "repositoryRoot",
    ),
    buildDirectory,
    buildDirectoryStat: await assertDirectoryWithoutSymlink(
      buildDirectory,
      "build",
    ),
    buildRoot: canonicalRoot,
    buildRootStat: await assertDirectoryWithoutSymlink(
      canonicalRoot,
      "buildRoot",
    ),
    inputsRoot,
    inputsRootStat: await assertDirectoryWithoutSymlink(inputsRoot, "inputs"),
  });
  await assertSafeInputsRoot(inputsContext);
  return inputsContext;
}

function validateAbsolutePath(value, label) {
  if (typeof value !== "string" || value.length === 0 || !isAbsolute(value)) {
    throw new TypeError(`${label} должен быть непустым абсолютным путем`);
  }
}

function assertSameResolvedPath(expectedPath, actualPath, message) {
  const relativePath = relative(expectedPath, actualPath);
  if (relativePath !== "" || isAbsolute(relativePath)) {
    throw new Error(message);
  }
}

async function ensureDirectChildDirectory(parentPath, childName, label) {
  const parentBefore = await assertDirectoryWithoutSymlink(
    parentPath,
    `${label} parent`,
  );
  const canonicalParent = await realpath(parentPath);
  const childPath = join(canonicalParent, childName);

  try {
    await mkdir(childPath);
  } catch (error) {
    if (error.code !== "EEXIST") {
      throw error;
    }
  }

  const parentAfter = await assertDirectoryWithoutSymlink(
    parentPath,
    `${label} parent`,
  );
  assertSameFileIdentity(
    parentBefore,
    parentAfter,
    `${label} parent изменен во время создания дочернего каталога`,
  );
  const childBefore = await assertDirectoryWithoutSymlink(childPath, label);
  const canonicalChild = await realpath(childPath);
  const childAfter = await assertDirectoryWithoutSymlink(childPath, label);
  assertSameFileIdentity(
    childBefore,
    childAfter,
    `${label} изменен во время проверки`,
  );

  const relativePath = relative(canonicalParent, canonicalChild);
  if (
    relativePath !== childName ||
    relativePath.startsWith(`..${sep}`) ||
    isAbsolute(relativePath)
  ) {
    throw new Error(`${label} отклонен: обнаружен выход за родительский каталог`);
  }
  return canonicalChild;
}

async function assertSafeInputsRoot(inputsContext) {
  const directories = [
    {
      path: inputsContext.repositoryRoot,
      pinnedStat: inputsContext.repositoryStat,
      label: "repositoryRoot",
      parentPath: null,
      childName: null,
    },
    {
      path: inputsContext.buildDirectory,
      pinnedStat: inputsContext.buildDirectoryStat,
      label: "build",
      parentPath: inputsContext.repositoryRoot,
      childName: BUILD_DIRECTORY_NAME,
    },
    {
      path: inputsContext.buildRoot,
      pinnedStat: inputsContext.buildRootStat,
      label: "buildRoot",
      parentPath: inputsContext.buildDirectory,
      childName: TOOL_BUILD_DIRECTORY_NAME,
    },
    {
      path: inputsContext.inputsRoot,
      pinnedStat: inputsContext.inputsRootStat,
      label: "inputs",
      parentPath: inputsContext.buildRoot,
      childName: INPUTS_DIRECTORY_NAME,
    },
  ];

  for (const directory of directories) {
    const before = await assertDirectoryWithoutSymlink(
      directory.path,
      directory.label,
    );
    assertSameFileIdentity(
      directory.pinnedStat,
      before,
      `${directory.label} изменен после подготовки inputs`,
    );

    const canonicalPath = await realpath(directory.path);
    const after = await assertDirectoryWithoutSymlink(
      directory.path,
      directory.label,
    );
    assertSameFileIdentity(
      directory.pinnedStat,
      after,
      `${directory.label} изменен во время проверки`,
    );
    assertSameResolvedPath(
      directory.path,
      canonicalPath,
      `${directory.label} отклонен: canonical path изменен`,
    );
    assertOutsidePublishedResources(canonicalPath);

    if (directory.parentPath !== null) {
      const relativePath = relative(directory.parentPath, canonicalPath);
      if (
        relativePath !== directory.childName ||
        relativePath.startsWith(`..${sep}`) ||
        isAbsolute(relativePath)
      ) {
        throw new Error(
          `${directory.label} отклонен: обнаружен выход за родительский каталог`,
        );
      }
    }
  }

  // Повторный проход ловит замену предка, случившуюся во время проверки цепочки.
  for (const directory of directories.reverse()) {
    const current = await assertDirectoryWithoutSymlink(
      directory.path,
      directory.label,
    );
    assertSameFileIdentity(
      directory.pinnedStat,
      current,
      `${directory.label} изменен во время проверки цепочки inputs`,
    );
  }
}

async function assertDirectoryWithoutSymlink(directoryPath, label) {
  const pathStat = await lstat(directoryPath, { bigint: true });
  if (pathStat.isSymbolicLink()) {
    throw new Error(`${label} отклонен: symbolic link запрещен`);
  }
  if (!pathStat.isDirectory()) {
    throw new Error(`${label} отклонен: ожидался каталог`);
  }
  if (!hasStableFileIdentity(pathStat)) {
    throw new Error(
      `${label} отклонен: стабильный идентификатор каталога недоступен`,
    );
  }
  return pathStat;
}

function assertOutsidePublishedResources(candidatePath) {
  const segments = resolve(candidatePath)
    .split(/[\\/]+/)
    .filter(Boolean)
    .map((segment) => segment.toLowerCase());

  for (let index = 0; index <= segments.length - 3; index += 1) {
    if (
      segments[index] === "src" &&
      segments[index + 1] === "main" &&
      segments[index + 2] === "resources"
    ) {
      throw new Error(
        "Source acquisition не может использовать src/main/resources как buildRoot",
      );
    }
  }
}

async function removeOwnedPath(filePath, ownedFileStat) {
  // Node.js не дает атомарный compare-and-unlink относительно открытого handle.
  // UUID и create-only open изолируют штатные запуски; если identity уже
  // отличается, не пытаемся удалить обнаруженный по имени чужой path.
  let pathStat;
  try {
    pathStat = await lstat(filePath, { bigint: true });
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    }
    throw error;
  }

  if (!isSameFileIdentity(pathStat, ownedFileStat)) {
    return false;
  }

  try {
    await unlink(filePath);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

async function cancelResponseBody(response) {
  try {
    if (typeof response?.body?.cancel === "function") {
      await response.body.cancel();
    } else if (typeof response?.body?.destroy === "function") {
      response.body.destroy();
    }
  } catch {
    // Сохраняем исходную ошибку проверки; body уже не используется.
  }
}
