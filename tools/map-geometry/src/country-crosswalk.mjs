const REGION_ID_PATTERN = /^country:[a-z0-9][a-z0-9-]{0,63}$/;
const CROSSWALK_VERSION_PATTERN = /^country-crosswalk-v[1-9][0-9]*$/;
const GDELT_COUNTRY_CODE_PATTERN = /^[A-Z0-9]{2}$/;
const NATURAL_EARTH_FEATURE_ID_PATTERN = /^(0|[1-9][0-9]*)$/;
const SHA_256_PATTERN = /^[a-f0-9]{64}$/;

const TOP_LEVEL_FIELDS = [
  "crosswalkSchemaVersion",
  "crosswalkVersion",
  "naturalEarthSourceId",
  "gdeltCountryLookupSourceId",
  "naturalEarthFeatureKey",
  "naturalEarthFeatureCoverage",
  "regions",
  "mappedLegacyGdeltCountryCodeEvidence",
  "unmappedGdeltCountryCodes",
];

const REGION_FIELDS = [
  "regionId",
  "displayName",
  "naturalEarthFeatureIds",
  "gdeltCountryCodes",
  "disputeStatus",
  "disputeSource",
];

const UNMAPPED_FIELDS = ["code", "lookupName", "reason"];
const MAPPED_LEGACY_EVIDENCE_FIELDS = [
  "code",
  "regionId",
  "reason",
  "evidence",
];
const LEGACY_EVIDENCE_FIELDS = [
  "kind",
  "datasetSha256",
  "observationCount",
  "observedValue",
  "description",
];

const DISPUTE_STATUSES = new Set(["STANDARD", "DISPUTED_DE_FACTO"]);
const MAPPED_LEGACY_REASONS = new Set(["VERIFIED_EXACT_REGION_MATCH"]);
const UNMAPPED_REASONS = new Set([
  "NO_TERRITORIAL_GEOMETRY",
  "SOURCE_GEOMETRY_NOT_INDEPENDENT",
  "AMBIGUOUS_MULTIPLE_REGIONS",
  "UNSUPPORTED_NON_COUNTRY_CODE",
]);

/** Ошибка означает, что crosswalk или один из его проверяемых источников нарушает строгий контракт. */
export class CountryCrosswalkValidationError extends TypeError {
  constructor(message) {
    super(message);
    this.name = "CountryCrosswalkValidationError";
  }
}

/**
 * Проверяет только структуру и внутреннюю уникальность crosswalk.
 *
 * Эта проверка не доказывает покрытие источников. Для этого нужна
 * {@link validateCountryCrosswalk}, которая получает реальные входные данные.
 */
export function validateCountryCrosswalkStructure(crosswalk) {
  assertStrictObject(crosswalk, "crosswalk", TOP_LEVEL_FIELDS);

  assertExactValue(
    crosswalk.crosswalkSchemaVersion,
    1,
    "crosswalk.crosswalkSchemaVersion",
  );
  assertPattern(
    crosswalk.crosswalkVersion,
    CROSSWALK_VERSION_PATTERN,
    "crosswalk.crosswalkVersion",
    'ожидается версия формата "country-crosswalk-vN" с N >= 1',
  );
  assertTrimmedNonEmptyString(
    crosswalk.naturalEarthSourceId,
    "crosswalk.naturalEarthSourceId",
  );
  assertTrimmedNonEmptyString(
    crosswalk.gdeltCountryLookupSourceId,
    "crosswalk.gdeltCountryLookupSourceId",
  );
  assertExactValue(
    crosswalk.naturalEarthFeatureKey,
    "NE_ID",
    "crosswalk.naturalEarthFeatureKey",
  );
  assertExactValue(
    crosswalk.naturalEarthFeatureCoverage,
    "ALL",
    "crosswalk.naturalEarthFeatureCoverage",
  );
  assertArray(crosswalk.regions, "crosswalk.regions");
  assertArray(
    crosswalk.mappedLegacyGdeltCountryCodeEvidence,
    "crosswalk.mappedLegacyGdeltCountryCodeEvidence",
  );
  assertArray(
    crosswalk.unmappedGdeltCountryCodes,
    "crosswalk.unmappedGdeltCountryCodes",
  );

  const regionById = new Map();
  const regionByNaturalEarthFeatureId = new Map();
  const regionByGdeltCountryCode = new Map();

  for (const [regionIndex, region] of crosswalk.regions.entries()) {
    const path = `crosswalk.regions[${regionIndex}]`;
    assertStrictObject(region, path, REGION_FIELDS);

    assertPattern(
      region.regionId,
      REGION_ID_PATTERN,
      `${path}.regionId`,
      'ожидается стабильный ID формата "country:<slug>"',
    );
    assertUniqueOwner(
      regionById,
      region.regionId,
      path,
      `${path}.regionId`,
      "regionId уже объявлен",
    );
    assertTrimmedNonEmptyString(region.displayName, `${path}.displayName`);

    assertArray(region.naturalEarthFeatureIds, `${path}.naturalEarthFeatureIds`);
    if (region.naturalEarthFeatureIds.length === 0) {
      fail(`${path}.naturalEarthFeatureIds`, "массив не должен быть пустым");
    }

    const featureIdsInsideRegion = new Set();
    for (const [featureIndex, featureId] of region.naturalEarthFeatureIds.entries()) {
      const featurePath = `${path}.naturalEarthFeatureIds[${featureIndex}]`;
      assertPattern(
        featureId,
        NATURAL_EARTH_FEATURE_ID_PATTERN,
        featurePath,
        "ожидается каноническая неотрицательная десятичная строка",
      );
      assertUniqueValue(
        featureIdsInsideRegion,
        featureId,
        featurePath,
        "feature ID повторяется внутри региона",
      );
      assertUniqueOwner(
        regionByNaturalEarthFeatureId,
        featureId,
        region.regionId,
        featurePath,
        "Natural Earth feature уже назначен другому региону",
      );
    }

    assertArray(region.gdeltCountryCodes, `${path}.gdeltCountryCodes`);
    const codesInsideRegion = new Set();
    for (const [codeIndex, code] of region.gdeltCountryCodes.entries()) {
      const codePath = `${path}.gdeltCountryCodes[${codeIndex}]`;
      assertGdeltCountryCode(code, codePath);
      assertUniqueValue(
        codesInsideRegion,
        code,
        codePath,
        "GDELT code повторяется внутри региона",
      );
      assertUniqueOwner(
        regionByGdeltCountryCode,
        code,
        region.regionId,
        codePath,
        "GDELT code уже назначен другому региону",
      );
    }

    if (!DISPUTE_STATUSES.has(region.disputeStatus)) {
      fail(
        `${path}.disputeStatus`,
        'ожидается "STANDARD" или "DISPUTED_DE_FACTO"',
      );
    }
    if (region.disputeStatus === "STANDARD") {
      assertExactValue(region.disputeSource, null, `${path}.disputeSource`);
    } else {
      assertStableSourceId(region.disputeSource, `${path}.disputeSource`);
    }
  }

  const mappedLegacyGdeltCountryCodeEvidenceByCode = new Map();
  for (const [evidenceIndex, entry] of
    crosswalk.mappedLegacyGdeltCountryCodeEvidence.entries()) {
    const path = `crosswalk.mappedLegacyGdeltCountryCodeEvidence[${evidenceIndex}]`;
    assertStrictObject(entry, path, MAPPED_LEGACY_EVIDENCE_FIELDS);
    assertGdeltCountryCode(entry.code, `${path}.code`);
    assertPattern(
      entry.regionId,
      REGION_ID_PATTERN,
      `${path}.regionId`,
      'ожидается стабильный ID формата "country:<slug>"',
    );
    if (!MAPPED_LEGACY_REASONS.has(entry.reason)) {
      fail(
        `${path}.reason`,
        `неизвестная причина mapped legacy code ${formatValue(entry.reason)}`,
      );
    }
    validateLegacyEvidence(entry.evidence, entry.code, `${path}.evidence`);

    const mappedRegionId = regionByGdeltCountryCode.get(entry.code);
    if (mappedRegionId === undefined) {
      fail(
        `${path}.code`,
        `legacy evidence ссылается на code ${entry.code}, которого нет в gdeltCountryCodes`,
      );
    }
    if (mappedRegionId !== entry.regionId) {
      fail(
        `${path}.regionId`,
        `code ${entry.code} фактически назначен региону ${mappedRegionId}`,
      );
    }
    if (mappedLegacyGdeltCountryCodeEvidenceByCode.has(entry.code)) {
      fail(
        `${path}.code`,
        `mapped legacy evidence уже объявлен для code ${entry.code}`,
      );
    }
    mappedLegacyGdeltCountryCodeEvidenceByCode.set(entry.code, entry);
  }

  const unmappedGdeltCountryCodeByCode = new Map();
  for (const [unmappedIndex, entry] of crosswalk.unmappedGdeltCountryCodes.entries()) {
    const path = `crosswalk.unmappedGdeltCountryCodes[${unmappedIndex}]`;
    assertStrictObject(entry, path, UNMAPPED_FIELDS, ["evidence"]);
    assertGdeltCountryCode(entry.code, `${path}.code`);
    assertTrimmedNonEmptyString(entry.lookupName, `${path}.lookupName`);
    if (!UNMAPPED_REASONS.has(entry.reason)) {
      fail(
        `${path}.reason`,
        `неизвестная причина ${formatValue(entry.reason)}`,
      );
    }
    if (Object.hasOwn(entry, "evidence")) {
      validateLegacyEvidence(entry.evidence, entry.code, `${path}.evidence`);
    }
    if (regionByGdeltCountryCode.has(entry.code)) {
      fail(
        `${path}.code`,
        `GDELT code ${entry.code} одновременно mapped и unmapped`,
      );
    }
    if (unmappedGdeltCountryCodeByCode.has(entry.code)) {
      fail(`${path}.code`, `unmapped GDELT code уже объявлен: ${entry.code}`);
    }
    unmappedGdeltCountryCodeByCode.set(entry.code, entry);
  }

  return {
    crosswalk,
    regionById,
    regionByNaturalEarthFeatureId,
    regionByGdeltCountryCode,
    mappedLegacyGdeltCountryCodeEvidenceByCode,
    unmappedGdeltCountryCodeByCode,
  };
}

/**
 * Проверяет crosswalk относительно фактического Natural Earth GeoJSON и текста
 * официального GDELT country lookup.
 */
export function validateCountryCrosswalk({
  crosswalk,
  naturalEarthFeatureCollection,
  gdeltCountryLookupTsv,
}) {
  const structure = validateCountryCrosswalkStructure(crosswalk);
  const naturalEarthFeatureIds = collectNaturalEarthFeatureIds(
    naturalEarthFeatureCollection,
    crosswalk.naturalEarthFeatureKey,
  );

  for (const [featureId, regionId] of structure.regionByNaturalEarthFeatureId) {
    if (!naturalEarthFeatureIds.has(featureId)) {
      fail(
        "crosswalk.regions",
        `Natural Earth feature ${featureId}, назначенный региону ${regionId}, отсутствует в источнике`,
      );
    }
  }
  for (const featureId of naturalEarthFeatureIds) {
    if (!structure.regionByNaturalEarthFeatureId.has(featureId)) {
      fail(
        "crosswalk.naturalEarthFeatureCoverage",
        `Natural Earth feature ${featureId} не назначен ни одному региону при coverage ALL`,
      );
    }
  }

  const gdeltCountryLookup = parseGdeltCountryLookup(gdeltCountryLookupTsv);
  for (const [code, regionId] of structure.regionByGdeltCountryCode) {
    const legacyEvidence =
      structure.mappedLegacyGdeltCountryCodeEvidenceByCode.get(code);
    if (!gdeltCountryLookup.has(code) && legacyEvidence === undefined) {
      fail(
        "crosswalk.regions",
        `mapped GDELT code ${code} региона ${regionId} отсутствует в official lookup и требует exact legacy evidence`,
      );
    }
    if (gdeltCountryLookup.has(code) && legacyEvidence !== undefined) {
      const evidenceIndex =
        crosswalk.mappedLegacyGdeltCountryCodeEvidence.indexOf(legacyEvidence);
      fail(
        `crosswalk.mappedLegacyGdeltCountryCodeEvidence[${evidenceIndex}].code`,
        `code ${code} присутствует в official lookup и не является legacy exception`,
      );
    }
  }

  for (const [code, entry] of structure.unmappedGdeltCountryCodeByCode) {
    const entryIndex = crosswalk.unmappedGdeltCountryCodes.indexOf(entry);
    const entryPath = `crosswalk.unmappedGdeltCountryCodes[${entryIndex}]`;
    const officialLookupName = gdeltCountryLookup.get(code);
    if (officialLookupName !== undefined) {
      if (entry.lookupName !== officialLookupName) {
        fail(
          `${entryPath}.lookupName`,
          `ожидается точное official lookup name ${formatValue(officialLookupName)}`,
        );
      }
      if (Object.hasOwn(entry, "evidence")) {
        fail(
          `${entryPath}.evidence`,
          "evidence допустим только для legacy code, отсутствующего в official lookup",
        );
      }
    } else if (!Object.hasOwn(entry, "evidence")) {
      fail(
        `${entryPath}.evidence`,
        `legacy code ${code} отсутствует в official lookup и требует exact evidence`,
      );
    }
  }

  for (const code of gdeltCountryLookup.keys()) {
    const mapped = structure.regionByGdeltCountryCode.has(code);
    const unmapped = structure.unmappedGdeltCountryCodeByCode.has(code);
    if (!mapped && !unmapped) {
      fail(
        "crosswalk",
        `official GDELT code ${code} должен быть ровно один раз mapped или явно unmapped`,
      );
    }
  }

  return {
    ...structure,
    naturalEarthFeatureIds,
    gdeltCountryLookup,
  };
}

/** Разбирает официальный двухколоночный GDELT country lookup без нормализации значений. */
export function parseGdeltCountryLookup(tsv) {
  if (typeof tsv !== "string") {
    fail("gdeltCountryLookupTsv", "ожидается строка TSV");
  }

  const withoutBom = tsv.startsWith("\uFEFF") ? tsv.slice(1) : tsv;
  const lines = withoutBom.split(/\r\n|\n|\r/u);
  while (lines.at(-1) === "") {
    lines.pop();
  }
  if (lines.length === 0) {
    fail("gdeltCountryLookupTsv", "lookup не должен быть пустым");
  }

  const lookup = new Map();
  for (const [lineIndex, line] of lines.entries()) {
    const path = `gdeltCountryLookupTsv:${lineIndex + 1}`;
    if (line.length === 0) {
      fail(path, "пустая строка внутри lookup недопустима");
    }
    const columns = line.split("\t");
    if (columns.length !== 2) {
      fail(path, "ожидаются ровно две колонки: code и lookup name");
    }
    const [code, lookupName] = columns;
    assertGdeltCountryCode(code, `${path}.code`);
    assertTrimmedNonEmptyString(lookupName, `${path}.lookupName`);
    if (lookup.has(code)) {
      fail(`${path}.code`, `official GDELT code ${code} повторяется`);
    }
    lookup.set(code, lookupName);
  }
  return lookup;
}

function collectNaturalEarthFeatureIds(featureCollection, featureKey) {
  if (!isObject(featureCollection)) {
    fail("naturalEarthFeatureCollection", "ожидается объект GeoJSON");
  }
  if (featureCollection.type !== "FeatureCollection") {
    fail(
      "naturalEarthFeatureCollection.type",
      'ожидается "FeatureCollection"',
    );
  }
  assertArray(
    featureCollection.features,
    "naturalEarthFeatureCollection.features",
  );

  const featureIds = new Set();
  for (const [featureIndex, feature] of featureCollection.features.entries()) {
    const path = `naturalEarthFeatureCollection.features[${featureIndex}]`;
    if (!isObject(feature)) {
      fail(path, "ожидается GeoJSON Feature object");
    }
    if (!isObject(feature.properties)) {
      fail(`${path}.properties`, "ожидается объект properties");
    }
    if (!Object.hasOwn(feature.properties, featureKey)) {
      fail(`${path}.properties.${featureKey}`, "обязательный feature key отсутствует");
    }

    const featureId = toCanonicalNaturalEarthFeatureId(
      feature.properties[featureKey],
      `${path}.properties.${featureKey}`,
    );
    assertUniqueValue(
      featureIds,
      featureId,
      `${path}.properties.${featureKey}`,
      "Natural Earth source содержит duplicate feature ID",
    );
  }
  return featureIds;
}

function toCanonicalNaturalEarthFeatureId(value, path) {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      fail(path, "ожидается безопасное неотрицательное целое число");
    }
    return String(value);
  }
  assertPattern(
    value,
    NATURAL_EARTH_FEATURE_ID_PATTERN,
    path,
    "ожидается каноническая неотрицательная десятичная строка или целое число",
  );
  return value;
}

function validateLegacyEvidence(evidence, code, path) {
  assertStrictObject(evidence, path, LEGACY_EVIDENCE_FIELDS);
  assertExactValue(
    evidence.kind,
    "VERIFIED_DATASET_OBSERVATION",
    `${path}.kind`,
  );
  assertPattern(
    evidence.datasetSha256,
    SHA_256_PATTERN,
    `${path}.datasetSha256`,
    "ожидается lowercase SHA-256 из 64 hex-символов",
  );
  if (!Number.isSafeInteger(evidence.observationCount) || evidence.observationCount <= 0) {
    fail(`${path}.observationCount`, "ожидается положительное безопасное целое число");
  }
  assertExactValue(evidence.observedValue, code, `${path}.observedValue`);
  assertTrimmedNonEmptyString(evidence.description, `${path}.description`);
}

function assertStrictObject(value, path, requiredFields, optionalFields = []) {
  if (!isObject(value)) {
    fail(path, "ожидается объект");
  }
  const allowedFields = new Set([...requiredFields, ...optionalFields]);
  for (const field of Object.keys(value)) {
    if (!allowedFields.has(field)) {
      fail(`${path}.${field}`, "поле не входит в строгую schema");
    }
  }
  for (const field of requiredFields) {
    if (!Object.hasOwn(value, field)) {
      fail(`${path}.${field}`, "обязательное поле отсутствует");
    }
  }
}

function assertArray(value, path) {
  if (!Array.isArray(value)) {
    fail(path, "ожидается массив");
  }
}

function assertTrimmedNonEmptyString(value, path) {
  if (typeof value !== "string" || value.length === 0 || value !== value.trim()) {
    fail(path, "ожидается непустая строка без пробелов по краям");
  }
}

function assertStableSourceId(value, path) {
  assertTrimmedNonEmptyString(value, path);
  if (/\s/u.test(value)) {
    fail(path, "stable source ID не должен содержать пробелы");
  }
}

function assertPattern(value, pattern, path, expectation) {
  if (typeof value !== "string" || !pattern.test(value)) {
    fail(path, expectation);
  }
}

function assertGdeltCountryCode(code, path) {
  assertPattern(
    code,
    GDELT_COUNTRY_CODE_PATTERN,
    path,
    "ожидается exact uppercase GDELT country code формата [A-Z0-9]{2}",
  );
}

function assertExactValue(value, expected, path) {
  if (value !== expected) {
    fail(path, `ожидается ${formatValue(expected)}, получено ${formatValue(value)}`);
  }
}

function assertUniqueValue(set, value, path, message) {
  if (set.has(value)) {
    fail(path, `${message}: ${value}`);
  }
  set.add(value);
}

function assertUniqueOwner(map, value, owner, path, message) {
  if (map.has(value)) {
    fail(path, `${message}: ${value}; первое назначение ${map.get(value)}`);
  }
  map.set(value, owner);
}

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatValue(value) {
  return JSON.stringify(value);
}

function fail(path, message) {
  throw new CountryCrosswalkValidationError(`${path}: ${message}`);
}
