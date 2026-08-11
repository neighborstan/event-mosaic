import { readFile } from "node:fs/promises";

export const SOURCE_MANIFEST_SCHEMA_VERSION = "1";

const SOURCE_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;
const FILE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const SHA_256_PATTERN = /^[a-f0-9]{64}$/;
const COMMIT_PATTERN = /^[a-f0-9]{40}$/;
const CONTENT_TYPE_PATTERN = /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/;
const ALLOWED_CONTENT_TYPES = new Set([
  "application/geo+json",
  "application/json",
  "application/octet-stream",
  "text/csv",
  "text/plain",
  "text/tab-separated-values",
]);

const MANIFEST_KEYS = ["manifestSchemaVersion", "sources"];
const SOURCE_KEYS = [
  "sourceId",
  "fileName",
  "url",
  "providerIdentity",
  "repositoryCommit",
  "license",
  "sha256",
  "maxBytes",
  "contentTypes",
];
const LICENSE_KEYS = ["name", "url"];

export function validateSourceManifest(candidate) {
  assertPlainObject(candidate, "корень");
  assertExactKeys(candidate, MANIFEST_KEYS, "корень");

  if (candidate.manifestSchemaVersion !== SOURCE_MANIFEST_SCHEMA_VERSION) {
    invalid(
      "manifestSchemaVersion",
      `должен быть равен \"${SOURCE_MANIFEST_SCHEMA_VERSION}\"`,
    );
  }
  if (!Array.isArray(candidate.sources) || candidate.sources.length === 0) {
    invalid("sources", "должен быть непустым массивом");
  }

  const sourceIds = new Set();
  const fileNames = new Set();
  const sources = candidate.sources.map((source, index) => {
    const validated = validateSource(source, `sources[${index}]`);
    assertUnique(sourceIds, validated.sourceId, `sources[${index}].sourceId`);
    assertUnique(fileNames, validated.fileName, `sources[${index}].fileName`);
    return Object.freeze(validated);
  });

  return Object.freeze({
    manifestSchemaVersion: SOURCE_MANIFEST_SCHEMA_VERSION,
    sources: Object.freeze(sources),
  });
}

export async function readSourceManifest(manifestPath) {
  assertNonEmptyString(manifestPath, "manifestPath");

  let parsed;
  try {
    parsed = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch (error) {
    throw new Error(`Не удалось прочитать source manifest: ${error.message}`, {
      cause: error,
    });
  }

  return validateSourceManifest(parsed);
}

function validateSource(source, fieldPath) {
  assertPlainObject(source, fieldPath);
  assertExactKeys(source, SOURCE_KEYS, fieldPath);

  assertPattern(source.sourceId, SOURCE_ID_PATTERN, `${fieldPath}.sourceId`);
  validateFileName(source.fileName, `${fieldPath}.fileName`);
  validateExactHttpsUrl(source.url, `${fieldPath}.url`);
  assertStableText(
    source.providerIdentity,
    `${fieldPath}.providerIdentity`,
  );

  if (
    source.repositoryCommit !== null &&
    (typeof source.repositoryCommit !== "string" ||
      !COMMIT_PATTERN.test(source.repositoryCommit))
  ) {
    invalid(
      `${fieldPath}.repositoryCommit`,
      "должен быть null или точным lowercase Git commit из 40 hex-символов",
    );
  }

  const license = validateLicense(source.license, `${fieldPath}.license`);
  assertPattern(source.sha256, SHA_256_PATTERN, `${fieldPath}.sha256`);

  if (!Number.isSafeInteger(source.maxBytes) || source.maxBytes <= 0) {
    invalid(`${fieldPath}.maxBytes`, "должен быть положительным целым числом");
  }

  const contentTypes = validateContentTypes(
    source.contentTypes,
    `${fieldPath}.contentTypes`,
  );

  return {
    sourceId: source.sourceId,
    fileName: source.fileName,
    url: source.url,
    providerIdentity: source.providerIdentity,
    repositoryCommit: source.repositoryCommit,
    license,
    sha256: source.sha256,
    maxBytes: source.maxBytes,
    contentTypes,
  };
}

function validateLicense(license, fieldPath) {
  assertPlainObject(license, fieldPath);
  assertExactKeys(license, LICENSE_KEYS, fieldPath);
  assertStableText(license.name, `${fieldPath}.name`);
  validateExactHttpsUrl(license.url, `${fieldPath}.url`);
  return Object.freeze({ name: license.name, url: license.url });
}

function validateContentTypes(contentTypes, fieldPath) {
  if (!Array.isArray(contentTypes) || contentTypes.length === 0) {
    invalid(fieldPath, "должен быть непустым массивом");
  }

  const seen = new Set();
  const validated = contentTypes.map((contentType, index) => {
    const itemPath = `${fieldPath}[${index}]`;
    if (
      typeof contentType !== "string" ||
      !CONTENT_TYPE_PATTERN.test(contentType) ||
      !ALLOWED_CONTENT_TYPES.has(contentType)
    ) {
      invalid(itemPath, "содержит неподдерживаемый media type");
    }
    assertUnique(seen, contentType, itemPath);
    return contentType;
  });

  return Object.freeze(validated);
}

function validateFileName(fileName, fieldPath) {
  if (
    typeof fileName !== "string" ||
    !FILE_NAME_PATTERN.test(fileName) ||
    fileName === "." ||
    fileName === ".." ||
    fileName.includes("/") ||
    fileName.includes("\\")
  ) {
    invalid(fieldPath, "должен быть безопасным basename без каталогов");
  }
}

function validateExactHttpsUrl(value, fieldPath) {
  assertNonEmptyString(value, fieldPath);

  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    invalid(fieldPath, "должен быть точным абсолютным HTTPS URL");
  }

  if (
    parsed.protocol !== "https:" ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    parsed.hash !== "" ||
    parsed.href !== value
  ) {
    invalid(
      fieldPath,
      "должен быть точным каноническим HTTPS URL без credentials и fragment",
    );
  }
}

function assertPlainObject(value, fieldPath) {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    (Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null)
  ) {
    invalid(fieldPath, "должен быть JSON object");
  }
}

function assertExactKeys(value, expectedKeys, fieldPath) {
  const actualKeys = Object.keys(value).sort();
  const sortedExpected = [...expectedKeys].sort();
  if (
    actualKeys.length !== sortedExpected.length ||
    actualKeys.some((key, index) => key !== sortedExpected[index])
  ) {
    invalid(
      fieldPath,
      `должен содержать ровно поля: ${sortedExpected.join(", ")}`,
    );
  }
}

function assertNonEmptyString(value, fieldPath) {
  if (typeof value !== "string" || value.length === 0) {
    invalid(fieldPath, "должен быть непустой строкой");
  }
}

function assertStableText(value, fieldPath) {
  assertNonEmptyString(value, fieldPath);
  if (value.trim() !== value || value.length > 256) {
    invalid(fieldPath, "должен быть строкой без внешних пробелов до 256 символов");
  }
}

function assertPattern(value, pattern, fieldPath) {
  if (typeof value !== "string" || !pattern.test(value)) {
    invalid(fieldPath, `не соответствует формату ${pattern}`);
  }
}

function assertUnique(seen, value, fieldPath) {
  if (seen.has(value)) {
    invalid(fieldPath, `дублирует значение \"${value}\"`);
  }
  seen.add(value);
}

function invalid(fieldPath, message) {
  throw new TypeError(`Некорректный source manifest: ${fieldPath} ${message}`);
}
