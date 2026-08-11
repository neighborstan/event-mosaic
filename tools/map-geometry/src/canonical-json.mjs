import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";

const UTF_8_DECODER = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: false,
});

export function encodeCanonicalJson(value) {
  return Buffer.from(`${JSON.stringify(value)}\n`, "utf8");
}

export function parseCanonicalJson(bytes, label) {
  if (!Buffer.isBuffer(bytes)) {
    throw new TypeError(`${label}: ожидается Buffer`);
  }

  let text;
  try {
    text = UTF_8_DECODER.decode(bytes);
  } catch (error) {
    throw new TypeError(`${label}: ожидается строгий UTF-8`, { cause: error });
  }
  if (text.charCodeAt(0) === 0xfeff) {
    throw new TypeError(`${label}: UTF-8 BOM недопустим`);
  }
  if (!text.endsWith("\n") || text.endsWith("\n\n") || text.includes("\r")) {
    throw new TypeError(
      `${label}: ожидается ровно один завершающий LF без CR`,
    );
  }

  let value;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw new TypeError(`${label}: не удалось разобрать JSON: ${error.message}`, {
      cause: error,
    });
  }

  const canonicalBytes = encodeCanonicalJson(value);
  if (!bytes.equals(canonicalBytes)) {
    throw new TypeError(
      `${label}: JSON не соответствует canonical byte representation`,
    );
  }
  return value;
}

export function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

export function deterministicGzip(bytes, gzipConfiguration) {
  return gzipSync(bytes, {
    level: gzipConfiguration.level,
    mtime: gzipConfiguration.mtime,
  });
}

export function sortObjectKeys(value) {
  if (Array.isArray(value)) {
    return value.map(sortObjectKeys);
  }
  if (isPlainObject(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort((left, right) => left.localeCompare(right, "en"))
        .map((key) => [key, sortObjectKeys(value[key])]),
    );
  }
  return value;
}

function isPlainObject(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
