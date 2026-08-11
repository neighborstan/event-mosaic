package com.neighbor.eventmosaic.search;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Один раз при запуске читает manifest выбранной геометрии из classpath и
 * строит проверенный каталог для точного соединения событий с регионами.
 * Ошибка ресурса или структуры останавливает запуск вместо частичного
 * сопоставления данных.
 */
@Component
final class CountryGeometryCatalogLoader {

	private static final String SUPPORTED_MANIFEST_SCHEMA_VERSION = "1";
	private static final String RESOURCE_ROOT = "static/map/geometry/";
	private static final Pattern REGION_ID_PATTERN =
			Pattern.compile("country:[a-z0-9][a-z0-9-]{0,63}");
	private static final Pattern GDELT_COUNTRY_CODE_PATTERN =
			Pattern.compile("[A-Z0-9]{2}");
	private final CountryGeometryCatalog catalog;

	CountryGeometryCatalogLoader(
			ObjectMapper objectMapper,
			CountryMapSnapshotProperties properties,
			ResourceLoader resourceLoader
	) {
		Objects.requireNonNull(objectMapper, "objectMapper must not be null");
		Objects.requireNonNull(properties, "properties must not be null");
		Objects.requireNonNull(resourceLoader, "resourceLoader must not be null");
		String geometryVersion = properties.geometryVersion();
		Resource manifest = resourceLoader.getResource(
				ResourceLoader.CLASSPATH_URL_PREFIX
						+ RESOURCE_ROOT
						+ geometryVersion
						+ "/manifest.json");
		this.catalog = loadCatalog(objectMapper, geometryVersion, manifest);
	}

	CountryGeometryCatalog catalog() {
		return catalog;
	}

	static CountryGeometryCatalog loadCatalog(
			ObjectMapper objectMapper,
			String expectedGeometryVersion,
			Resource manifestResource
	) {
		Objects.requireNonNull(objectMapper, "objectMapper must not be null");
		Objects.requireNonNull(
				expectedGeometryVersion, "expectedGeometryVersion must not be null");
		Objects.requireNonNull(manifestResource, "manifestResource must not be null");
		try (InputStream input = manifestResource.getInputStream()) {
			JsonNode root = objectMapper.readTree(input);
			return parseCatalog(root, expectedGeometryVersion);
		}
		catch (JacksonException | IOException exception) {
			throw new IllegalStateException(
					"Cannot read country geometry catalog for " + expectedGeometryVersion,
					exception);
		}
	}

	private static CountryGeometryCatalog parseCatalog(
			JsonNode root,
			String expectedGeometryVersion
	) {
		requireObject(root, "manifest");
		String schemaVersion = requireString(
				root, "manifestSchemaVersion", "manifest.manifestSchemaVersion");
		if (!SUPPORTED_MANIFEST_SCHEMA_VERSION.equals(schemaVersion)) {
			throw invalid(
					"manifest.manifestSchemaVersion",
					"unsupported schema version " + schemaVersion);
		}
		String geometryVersion = requireString(
				root, "geometryVersion", "manifest.geometryVersion");
		if (!expectedGeometryVersion.equals(geometryVersion)) {
			throw invalid(
					"manifest.geometryVersion",
					"expected " + expectedGeometryVersion + " but found " + geometryVersion);
		}

		JsonNode regionsNode = requireArray(root, "regions", "manifest.regions");
		if (regionsNode.isEmpty()) {
			throw invalid("manifest.regions", "must not be empty");
		}
		var regions = new ArrayList<CountryGeometryCatalog.Region>();
		var regionIds = new java.util.HashSet<String>();
		var regionIdByCode = new LinkedHashMap<String, String>();
		String previousRegionId = null;
		for (int regionIndex = 0; regionIndex < regionsNode.size(); regionIndex++) {
			JsonNode regionNode = regionsNode.get(regionIndex);
			String regionPath = "manifest.regions[" + regionIndex + "]";
			requireObject(regionNode, regionPath);
			String regionId = requirePattern(
					requireString(regionNode, "regionId", regionPath + ".regionId"),
					REGION_ID_PATTERN,
					regionPath + ".regionId");
			if (!regionIds.add(regionId)) {
				throw invalid(regionPath + ".regionId", "duplicate regionId " + regionId);
			}
			requireStrictlyIncreasing(
					previousRegionId, regionId, regionPath + ".regionId", "regionId");
			previousRegionId = regionId;

			JsonNode codesNode = requireArray(
					regionNode, "gdeltCountryCodes", regionPath + ".gdeltCountryCodes");
			List<String> regionCodes = readMappedCodes(
					codesNode, regionId, regionPath, regionIdByCode);
			regions.add(new CountryGeometryCatalog.Region(regionId, regionCodes));
		}

		JsonNode unmappedNode = requireArray(
				root,
				"unmappedGdeltCountryCodes",
				"manifest.unmappedGdeltCountryCodes");
		Map<String, CountryGeometryCatalog.UnmappedReason> unmappedReasons =
				readUnmappedCodes(unmappedNode, regionIdByCode);
		return new CountryGeometryCatalog(
				geometryVersion,
				regions,
				regionIdByCode,
				unmappedReasons);
	}

	private static List<String> readMappedCodes(
			JsonNode codesNode,
			String regionId,
			String regionPath,
			Map<String, String> regionIdByCode
	) {
		var regionCodes = new ArrayList<String>();
		var codesInsideRegion = new java.util.HashSet<String>();
		String previousCode = null;
		for (int codeIndex = 0; codeIndex < codesNode.size(); codeIndex++) {
			String codePath = regionPath + ".gdeltCountryCodes[" + codeIndex + "]";
			String code = requirePattern(
					requireString(codesNode.get(codeIndex), codePath),
					GDELT_COUNTRY_CODE_PATTERN,
					codePath);
			if (!codesInsideRegion.add(code)) {
				throw invalid(codePath, "duplicate GDELT code " + code);
			}
			requireStrictlyIncreasing(previousCode, code, codePath, "GDELT code");
			previousCode = code;
			String previousOwner = regionIdByCode.putIfAbsent(code, regionId);
			if (previousOwner != null) {
				throw invalid(
						codePath,
						"GDELT code " + code + " already belongs to " + previousOwner);
			}
			regionCodes.add(code);
		}
		return List.copyOf(regionCodes);
	}

	private static Map<String, CountryGeometryCatalog.UnmappedReason> readUnmappedCodes(
			JsonNode unmappedNode,
			Map<String, String> regionIdByCode
	) {
		var unmappedReasons =
				new LinkedHashMap<String, CountryGeometryCatalog.UnmappedReason>();
		String previousCode = null;
		for (int entryIndex = 0; entryIndex < unmappedNode.size(); entryIndex++) {
			JsonNode entryNode = unmappedNode.get(entryIndex);
			String entryPath = "manifest.unmappedGdeltCountryCodes[" + entryIndex + "]";
			requireObject(entryNode, entryPath);
			String code = requirePattern(
					requireString(entryNode, "code", entryPath + ".code"),
					GDELT_COUNTRY_CODE_PATTERN,
					entryPath + ".code");
			if (unmappedReasons.containsKey(code)) {
				throw invalid(entryPath + ".code", "duplicate unmapped code " + code);
			}
			requireStrictlyIncreasing(previousCode, code, entryPath + ".code", "unmapped code");
			previousCode = code;
			if (regionIdByCode.containsKey(code)) {
				throw invalid(entryPath + ".code", "code " + code + " is both mapped and unmapped");
			}
			String reasonValue = requireString(entryNode, "reason", entryPath + ".reason");
			CountryGeometryCatalog.UnmappedReason reason;
			try {
				reason = CountryGeometryCatalog.UnmappedReason.valueOf(reasonValue);
			}
			catch (IllegalArgumentException exception) {
				throw invalid(entryPath + ".reason", "unsupported reason " + reasonValue);
			}
			unmappedReasons.put(code, reason);
		}
		return Map.copyOf(unmappedReasons);
	}

	private static JsonNode requireArray(JsonNode owner, String field, String path) {
		JsonNode value = owner.get(field);
		if (value == null || !value.isArray()) {
			throw invalid(path, "must be an array");
		}
		return value;
	}

	private static String requireString(JsonNode owner, String field, String path) {
		JsonNode value = owner.get(field);
		if (value == null) {
			throw invalid(path, "must be present");
		}
		return requireString(value, path);
	}

	private static String requireString(JsonNode value, String path) {
		if (!value.isString()) {
			throw invalid(path, "must be a string");
		}
		String text = value.asString();
		if (text.isBlank() || !text.equals(text.trim())) {
			throw invalid(path, "must be a trimmed non-empty string");
		}
		return text;
	}

	private static String requirePattern(String value, Pattern pattern, String path) {
		if (!pattern.matcher(value).matches()) {
			throw invalid(path, "has unsupported format " + value);
		}
		return value;
	}

	private static void requireObject(JsonNode value, String path) {
		if (value == null || !value.isObject()) {
			throw invalid(path, "must be an object");
		}
	}

	private static void requireStrictlyIncreasing(
			String previous,
			String current,
			String path,
			String label
	) {
		if (previous != null && previous.compareTo(current) >= 0) {
			throw invalid(path, label + " values must use canonical ascending order");
		}
	}

	private static IllegalStateException invalid(String path, String detail) {
		return new IllegalStateException(
				"Invalid country geometry catalog at " + path + ": " + detail);
	}
}
