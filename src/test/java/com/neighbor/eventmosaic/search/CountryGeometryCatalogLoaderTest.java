package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

@DisplayName("Загрузка каталога регионов из опубликованного manifest")
class CountryGeometryCatalogLoaderTest {

	private static final String GEOMETRY_VERSION = "country-v1";
	private static final String MANIFEST_RESOURCE =
			"static/map/geometry/country-v1/manifest.json";
	private static final String FIXTURE_RESOURCE =
			"search/country-geometry/valid-manifest.json";

	private final ObjectMapper objectMapper = JsonMapper.builder().build();

	@Test
	@DisplayName("Настоящий каталог сохраняет полный состав регионов и все принятые коды")
	void loadsPublishedCountryV1Catalog() {
		CountryGeometryCatalog catalog = new CountryGeometryCatalogLoader(
				objectMapper,
				properties(),
				new DefaultResourceLoader())
				.catalog();

		assertThat(catalog.geometryVersion()).isEqualTo(GEOMETRY_VERSION);
		assertThat(catalog.regions()).hasSize(258);
		assertThat(catalog.regions())
				.extracting(CountryGeometryCatalog.Region::regionId)
				.isSorted();
		assertThat(catalog.regions())
				.filteredOn(region -> region.gdeltCountryCodes().isEmpty())
				.hasSize(17);
		assertThat(catalog.regionIdByGdeltCountryCode())
				.hasSize(242)
				.containsEntry("GZ", "country:psx")
				.containsEntry("WE", "country:psx");
		assertThat(catalog.unmappedReasonByGdeltCountryCode())
				.hasSize(33)
				.containsEntry(
						"NT",
						CountryGeometryCatalog.UnmappedReason.AMBIGUOUS_MULTIPLE_REGIONS)
				.containsEntry(
						"OC",
						CountryGeometryCatalog.UnmappedReason.UNSUPPORTED_NON_COUNTRY_CODE);
		assertThat(catalog.unmappedReasonByGdeltCountryCode().values())
				.containsOnly(CountryGeometryCatalog.UnmappedReason.values());
		assertThat(catalog.unmappedReasonByGdeltCountryCode().values())
				.filteredOn(reason -> reason
						== CountryGeometryCatalog.UnmappedReason.AMBIGUOUS_MULTIPLE_REGIONS)
				.hasSize(2);
		assertThat(catalog.unmappedReasonByGdeltCountryCode().values())
				.filteredOn(reason -> reason
						== CountryGeometryCatalog.UnmappedReason.NO_TERRITORIAL_GEOMETRY)
				.hasSize(8);
		assertThat(catalog.unmappedReasonByGdeltCountryCode().values())
				.filteredOn(reason -> reason
						== CountryGeometryCatalog.UnmappedReason.SOURCE_GEOMETRY_NOT_INDEPENDENT)
				.hasSize(18);
		assertThat(catalog.unmappedReasonByGdeltCountryCode().values())
				.filteredOn(reason -> reason
						== CountryGeometryCatalog.UnmappedReason.UNSUPPORTED_NON_COUNTRY_CODE)
				.hasSize(5);
		assertThat(catalog.knownGdeltCountryCodes()).hasSize(275);
	}

	@Test
	@DisplayName("Останавливает загрузку, когда manifest отсутствует в classpath")
	void rejectsMissingClasspathResource() {
		var missingResource = new ClassPathResource(
				"search/country-geometry/missing-manifest.json");

		assertThatThrownBy(() -> CountryGeometryCatalogLoader.loadCatalog(
				objectMapper,
				GEOMETRY_VERSION,
				missingResource))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Cannot read country geometry catalog");
	}

	@Test
	@DisplayName("Останавливает загрузку, когда manifest содержит поврежденный JSON")
	void rejectsMalformedJson() {
		var malformedResource = new ByteArrayResource(
				"{\"manifestSchemaVersion\":"
						.getBytes(StandardCharsets.UTF_8));

		assertThatThrownBy(() -> CountryGeometryCatalogLoader.loadCatalog(
				objectMapper,
				GEOMETRY_VERSION,
				malformedResource))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Cannot read country geometry catalog");
	}

	@Test
	@DisplayName("Каталог и вложенные списки нельзя изменить после загрузки")
	void keepsCatalogCollectionsImmutable() throws Exception {
		CountryGeometryCatalog catalog = loadFixture(validManifest());

		assertThatThrownBy(() -> catalog.regions().clear())
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> catalog.regions().getFirst().gdeltCountryCodes().clear())
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> catalog.regionIdByGdeltCountryCode().clear())
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> catalog.unmappedReasonByGdeltCountryCode().clear())
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	@DisplayName("Отклоняет другую схему и несовпадающую версию геометрии")
	void rejectsSchemaAndGeometryVersionMismatch() throws Exception {
		ObjectNode differentSchema = validManifest();
		differentSchema.put("manifestSchemaVersion", "2");
		assertInvalid(differentSchema, "manifest.manifestSchemaVersion");

		ObjectNode numericSchema = validManifest();
		numericSchema.put("manifestSchemaVersion", 1);
		assertInvalid(numericSchema, "manifest.manifestSchemaVersion");

		ObjectNode differentGeometryVersion = validManifest();
		differentGeometryVersion.put("geometryVersion", "country-v2");
		assertInvalid(differentGeometryVersion, "manifest.geometryVersion");
	}

	@Test
	@DisplayName("Отклоняет повторенный, отсутствующий или неупорядоченный регион")
	void rejectsInvalidRegionRoster() throws Exception {
		ObjectNode duplicateRegion = validManifest();
		ArrayNode duplicateRegions = regions(duplicateRegion);
		duplicateRegions.set(1, duplicateRegions.get(0).deepCopy());
		assertInvalid(duplicateRegion, "duplicate regionId");

		ObjectNode missingRegion = validManifest();
		region(missingRegion, 0).remove("regionId");
		assertInvalid(missingRegion, "manifest.regions[0].regionId");

		ObjectNode unorderedRegions = validManifest();
		ArrayNode reordered = regions(unorderedRegions);
		JsonNode first = reordered.remove(0);
		reordered.insert(1, first);
		assertInvalid(unorderedRegions, "canonical ascending order");
	}

	@Test
	@DisplayName("Различает разрешенный регион без кода и отсутствующий список кодов")
	void requiresMappedCodeArrayButAllowsItToBeEmpty() throws Exception {
		CountryGeometryCatalog catalog = loadFixture(validManifest());
		assertThat(catalog.regions())
				.filteredOn(region -> region.regionId().equals("country:bbb"))
				.singleElement()
				.extracting(CountryGeometryCatalog.Region::gdeltCountryCodes)
				.isEqualTo(List.of());

		ObjectNode missingCodes = validManifest();
		region(missingCodes, 0).remove("gdeltCountryCodes");
		assertInvalid(missingCodes, "manifest.regions[0].gdeltCountryCodes");
	}

	@Test
	@DisplayName("Отклоняет повторенный или отсутствующий код в любом наборе")
	void rejectsDuplicateAndMissingCodes() throws Exception {
		ObjectNode duplicateMappedCode = validManifest();
		codes(duplicateMappedCode, 1).add("AA");
		assertInvalid(duplicateMappedCode, "already belongs");

		ObjectNode mappedAndUnmappedCode = validManifest();
		unmapped(mappedAndUnmappedCode, 0).put("code", "AA");
		assertInvalid(mappedAndUnmappedCode, "both mapped and unmapped");

		ObjectNode missingMappedCode = validManifest();
		codes(missingMappedCode, 0).set(0, "");
		assertInvalid(missingMappedCode, "trimmed non-empty string");

		ObjectNode missingUnmappedCode = validManifest();
		unmapped(missingUnmappedCode, 0).remove("code");
		assertInvalid(missingUnmappedCode, "unmappedGdeltCountryCodes[0].code");
	}

	@Test
	@DisplayName("Отклоняет неизвестную причину и неканонический порядок кодов")
	void rejectsUnknownReasonAndUnorderedCodes() throws Exception {
		ObjectNode unknownReason = validManifest();
		unmapped(unknownReason, 0).put("reason", "UNKNOWN_REASON");
		assertInvalid(unknownReason, "unsupported reason");

		ObjectNode unorderedMappedCodes = validManifest();
		ArrayNode codes = codes(unorderedMappedCodes, 2);
		JsonNode firstCode = codes.remove(0);
		codes.add(firstCode);
		assertInvalid(unorderedMappedCodes, "canonical ascending order");

		ObjectNode unorderedUnmappedCodes = validManifest();
		ArrayNode entries = unmappedEntries(unorderedUnmappedCodes);
		JsonNode firstEntry = entries.remove(0);
		entries.add(firstEntry);
		assertInvalid(unorderedUnmappedCodes, "canonical ascending order");
	}

	@Test
	@DisplayName("Читает manifest из JAR classpath без преобразования ресурса в файл")
	void readsManifestFromJarClasspath(@TempDir Path temporaryDirectory) throws Exception {
		Path jarPath = temporaryDirectory.resolve("country-catalog-fixture.jar");
		byte[] fixtureBytes = new ClassPathResource(FIXTURE_RESOURCE)
				.getContentAsByteArray();
		try (JarOutputStream output = new JarOutputStream(
					java.nio.file.Files.newOutputStream(jarPath))) {
			output.putNextEntry(new JarEntry(MANIFEST_RESOURCE));
			output.write(fixtureBytes);
			output.closeEntry();
		}

		try (URLClassLoader jarClassLoader = new URLClassLoader(
				new java.net.URL[]{jarPath.toUri().toURL()},
				null)) {
			var resourceLoader = new DefaultResourceLoader(jarClassLoader);
			CountryGeometryCatalog catalog = new CountryGeometryCatalogLoader(
					objectMapper,
					properties(),
					resourceLoader)
					.catalog();

			assertThat(catalog.geometryVersion()).isEqualTo(GEOMETRY_VERSION);
			assertThat(catalog.regions()).hasSize(3);
			assertThat(resourceLoader.getResource(
					"classpath:" + MANIFEST_RESOURCE).isFile()).isFalse();
		}
	}

	private CountryGeometryCatalog loadFixture(ObjectNode manifest) throws Exception {
		return CountryGeometryCatalogLoader.loadCatalog(
				objectMapper,
				GEOMETRY_VERSION,
				new ByteArrayResource(objectMapper.writeValueAsBytes(manifest)));
	}

	private ObjectNode validManifest() throws Exception {
		try (InputStream input = new ClassPathResource(FIXTURE_RESOURCE).getInputStream()) {
			return (ObjectNode) objectMapper.readTree(input);
		}
	}

	private void assertInvalid(ObjectNode manifest, String messagePart) {
		assertThatThrownBy(() -> loadFixture(manifest))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining(messagePart);
	}

	private static ArrayNode regions(ObjectNode manifest) {
		return (ArrayNode) manifest.get("regions");
	}

	private static ObjectNode region(ObjectNode manifest, int index) {
		return (ObjectNode) regions(manifest).get(index);
	}

	private static ArrayNode codes(ObjectNode manifest, int regionIndex) {
		return (ArrayNode) region(manifest, regionIndex).get("gdeltCountryCodes");
	}

	private static ArrayNode unmappedEntries(ObjectNode manifest) {
		return (ArrayNode) manifest.get("unmappedGdeltCountryCodes");
	}

	private static ObjectNode unmapped(ObjectNode manifest, int index) {
		return (ObjectNode) unmappedEntries(manifest).get(index);
	}

	private static CountryMapSnapshotProperties properties() {
		return new CountryMapSnapshotProperties(
				GEOMETRY_VERSION,
				Duration.ofMinutes(15));
	}
}
