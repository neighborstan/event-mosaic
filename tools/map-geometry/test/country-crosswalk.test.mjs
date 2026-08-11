import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CountryCrosswalkValidationError,
  parseGdeltCountryLookup,
  validateCountryCrosswalk,
  validateCountryCrosswalkStructure,
} from "../src/country-crosswalk.mjs";

const fixtureUrl = (name) => new URL(`./fixtures/crosswalk/${name}`, import.meta.url);
const readJsonFixture = (name) => JSON.parse(readFileSync(fixtureUrl(name), "utf8"));
const readTextFixture = (name) => readFileSync(fixtureUrl(name), "utf8");

const naturalEarthFeatureCollection = readJsonFixture("natural-earth.geojson");
const gdeltCountryLookupTsv = readTextFixture("gdelt-country-lookup.tsv");
const validCrosswalk = readJsonFixture("valid-country-crosswalk.json");
const checkedInCrosswalk = JSON.parse(
  readFileSync(new URL("../data/country-crosswalk.json", import.meta.url), "utf8"),
);

function validate(crosswalk) {
  return validateCountryCrosswalk({
    crosswalk,
    naturalEarthFeatureCollection,
    gdeltCountryLookupTsv,
  });
}

test("Согласованная таблица полностью покрывает обе исходные записи", () => {
  const result = validate(validCrosswalk);

  assert.equal(result.regionById.size, 2);
  assert.equal(result.regionByNaturalEarthFeatureId.size, 2);
  assert.equal(result.regionByGdeltCountryCode.get("AA"), "country:alpha");
});

test("Структуру и внутреннюю уникальность можно проверить без исходных данных", () => {
  const result = validateCountryCrosswalkStructure(validCrosswalk);

  assert.equal(result.unmappedGdeltCountryCodeByCode.size, 1);
});

test("Поставляемая таблица соответствий проходит проверку схемы и уникальности", () => {
  const result = validateCountryCrosswalkStructure(checkedInCrosswalk);

  assert.equal(result.regionById.size, 258);
  assert.equal(result.regionByNaturalEarthFeatureId.size, 258);
  assert.equal(result.regionByGdeltCountryCode.size, 242);
  assert.equal(result.mappedLegacyGdeltCountryCodeEvidenceByCode.size, 0);
  assert.equal(result.unmappedGdeltCountryCodeByCode.size, 33);
  assert.equal(result.regionByGdeltCountryCode.get("GZ"), "country:psx");
  assert.equal(result.regionByGdeltCountryCode.get("WE"), "country:psx");

  const observedLegacyCode = result.unmappedGdeltCountryCodeByCode.get("OC");
  assert.deepEqual(observedLegacyCode.evidence, {
    kind: "VERIFIED_DATASET_OBSERVATION",
    datasetSha256:
      "75be83f05fd84eca2e3be29b0386a84e513b641c7ceac0792261a7d4cf972bbe",
    observationCount: 2,
    observedValue: "OC",
    description:
      "Verified GDELT Event sample used for the 2026-08-10 boundary coverage measurement",
  });
});

test("Явно неподдержанный официальный код можно оставить без сопоставления", () => {
  const result = validate(validCrosswalk);

  assert.equal(
    result.unmappedGdeltCountryCodeByCode.has("BB"),
    true,
  );
});

test("Таблица отклоняет объект, которого нет в источнике Natural Earth", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[0].naturalEarthFeatureIds = ["999"];

  assert.throws(
    () => validate(candidate),
    (error) =>
      error instanceof CountryCrosswalkValidationError &&
      /feature 999.*отсутствует в источнике/u.test(error.message),
  );
});

test("Таблица отклоняет повторяющийся стабильный regionId", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[1].regionId = candidate.regions[0].regionId;

  assert.throws(
    () => validateCountryCrosswalkStructure(candidate),
    /regionId уже объявлен/u,
  );
});

test("Строгая схема отклоняет незаявленное поле", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[0].joinByDisplayName = true;

  assert.throws(
    () => validateCountryCrosswalkStructure(candidate),
    /joinByDisplayName: поле не входит в строгую schema/u,
  );
});

test("Статус спорной территории и disputeSource задаются только согласованной парой", () => {
  const standardWithSource = structuredClone(validCrosswalk);
  standardWithSource.regions[0].disputeSource = "unexpected-source";
  assert.throws(
    () => validateCountryCrosswalkStructure(standardWithSource),
    /disputeSource: ожидается null/u,
  );

  const disputedWithoutSource = structuredClone(validCrosswalk);
  disputedWithoutSource.regions[1].disputeSource = null;
  assert.throws(
    () => validateCountryCrosswalkStructure(disputedWithoutSource),
    /disputeSource: ожидается непустая строка/u,
  );
});

test("Таблица отклоняет назначение одного объекта Natural Earth двум регионам", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[1].naturalEarthFeatureIds.push("101");

  assert.throws(
    () => validateCountryCrosswalkStructure(candidate),
    /feature уже назначен другому региону/u,
  );
});

test("Таблица отклоняет назначение одного кода GDELT двум регионам", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[1].gdeltCountryCodes.push("AA");

  assert.throws(
    () => validateCountryCrosswalkStructure(candidate),
    /GDELT code уже назначен другому региону/u,
  );
});

test("Таблица отклоняет отличие от точного официального названия", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.unmappedGdeltCountryCodes[0].lookupName = "Beta";

  assert.throws(
    () => validate(candidate),
    /ожидается точное official lookup name "Beta Territory"/u,
  );
});

test("Каждый официальный код GDELT должен быть сопоставлен или явно оставлен без региона", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.unmappedGdeltCountryCodes = [];

  assert.throws(
    () => validate(candidate),
    /official GDELT code BB должен быть ровно один раз mapped или явно unmapped/u,
  );
});

test("Код вне официального справочника можно сопоставить по отдельному точному свидетельству", () => {
  const candidate = structuredClone(validCrosswalk);
  candidate.regions[0].gdeltCountryCodes.push("ZZ");
  candidate.mappedLegacyGdeltCountryCodeEvidence.push({
    code: "ZZ",
    regionId: "country:alpha",
    reason: "VERIFIED_EXACT_REGION_MATCH",
    evidence: {
      kind: "VERIFIED_DATASET_OBSERVATION",
      datasetSha256: "b".repeat(64),
      observationCount: 4,
      observedValue: "ZZ",
      description: "Точная принадлежность к региону проверена по набору данных.",
    },
  });

  const result = validate(candidate);

  assert.equal(
    result.mappedLegacyGdeltCountryCodeEvidenceByCode.get("ZZ").regionId,
    "country:alpha",
  );
});

test("Свидетельство mapped legacy code обязано точно совпадать с фактическим mapping", () => {
  const withoutEvidence = structuredClone(validCrosswalk);
  withoutEvidence.regions[0].gdeltCountryCodes.push("ZZ");
  assert.throws(
    () => validate(withoutEvidence),
    /mapped GDELT code ZZ.*требует exact legacy evidence/u,
  );

  const wrongRegion = structuredClone(withoutEvidence);
  wrongRegion.mappedLegacyGdeltCountryCodeEvidence.push({
    code: "ZZ",
    regionId: "country:beta-territory",
    reason: "VERIFIED_EXACT_REGION_MATCH",
    evidence: {
      kind: "VERIFIED_DATASET_OBSERVATION",
      datasetSha256: "c".repeat(64),
      observationCount: 1,
      observedValue: "ZZ",
      description: "Проверяемое свидетельство с неверным регионом.",
    },
  });
  assert.throws(
    () => validateCountryCrosswalkStructure(wrongRegion),
    /code ZZ фактически назначен региону country:alpha/u,
  );

  const officialCode = structuredClone(validCrosswalk);
  officialCode.mappedLegacyGdeltCountryCodeEvidence.push({
    code: "AA",
    regionId: "country:alpha",
    reason: "VERIFIED_EXACT_REGION_MATCH",
    evidence: {
      kind: "VERIFIED_DATASET_OBSERVATION",
      datasetSha256: "d".repeat(64),
      observationCount: 1,
      observedValue: "AA",
      description: "Официальный код не должен оформляться как legacy exception.",
    },
  });
  assert.throws(
    () => validate(officialCode),
    /code AA присутствует в official lookup и не является legacy exception/u,
  );
});

test("Код вне официального справочника принимается только с проверяемым свидетельством", () => {
  const withoutEvidence = structuredClone(validCrosswalk);
  withoutEvidence.unmappedGdeltCountryCodes.push({
    code: "ZZ",
    lookupName: "Observed legacy value",
    reason: "UNSUPPORTED_NON_COUNTRY_CODE",
  });
  assert.throws(
    () => validate(withoutEvidence),
    /legacy code ZZ.*требует exact evidence/u,
  );

  const withEvidence = structuredClone(withoutEvidence);
  withEvidence.unmappedGdeltCountryCodes[1].evidence = {
    kind: "VERIFIED_DATASET_OBSERVATION",
    datasetSha256: "a".repeat(64),
    observationCount: 3,
    observedValue: "ZZ",
    description: "Код найден в проверенном наборе событий.",
  };

  assert.doesNotThrow(() => validate(withEvidence));
});

test("Разбор справочника сохраняет имена без неявного изменения", () => {
  const lookup = parseGdeltCountryLookup("AA\tAlpha Name\r\nBB\tBeta Name\r\n");

  assert.deepEqual([...lookup], [
    ["AA", "Alpha Name"],
    ["BB", "Beta Name"],
  ]);
});
