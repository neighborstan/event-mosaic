package com.neighbor.eventmosaic.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.ClassPathResource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(
		classes = MapStaticDeliveryIntegrationTest.StaticDeliveryTestApplication.class,
		webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
		properties = {
				"spring.docker.compose.enabled=false",
				"management.endpoint.health.validate-group-membership=false"
		})
@DisplayName("Поставка страницы карты и версионированной геометрии из Spring Boot")
class MapStaticDeliveryIntegrationTest {

	private static final String GEOMETRY_VERSION = "country-v1";
	private static final String RESOURCE_ROOT = "static/map/geometry/" + GEOMETRY_VERSION;
	private static final String HTTP_ROOT = "/map/geometry/" + GEOMETRY_VERSION;
	private static final String GEO_JSON_MEDIA_TYPE = "application/geo+json";
	private static final String JSON_MEDIA_TYPE = "application/json";

	@LocalServerPort
	private int serverPort;

	private HttpClient httpClient;

	@BeforeEach
	void createHttpClient() {
		httpClient = HttpClient.newHttpClient();
	}

	@AfterEach
	void closeHttpClient() {
		if (httpClient != null) {
			httpClient.close();
		}
	}

	@Test
	@DisplayName("Главная страница и собранные скрипты со стилями доступны с одного сервера")
	void servesFrontendAndItsHashedAssets() throws Exception {
		HttpResponse<byte[]> page = get("/");
		assertThat(page.statusCode()).isEqualTo(200);
		assertThat(page.headers().firstValue("Content-Type")).hasValueSatisfying(value ->
				assertThat(value).startsWith("text/html"));
		assertThat(page.body()).containsExactly(classPathBytes("static/index.html"));
		String html = new String(page.body(), StandardCharsets.UTF_8);
		assertThat(html).contains("id=\"root\"").doesNotContain("/@vite/client", "localhost:5173");
		var assets = Pattern.compile("(?:src|href)=\"(/assets/[^\"]+)\"")
				.matcher(html).results().map(match -> match.group(1)).toList();
		assertThat(assets).hasSize(2);
		assertThat(assets).anyMatch(path -> path.matches("/assets/.+-[\\w-]+\\.js"));
		assertThat(assets).anyMatch(path -> path.matches("/assets/.+-[\\w-]+\\.css"));
		for (String path : assets) {
			HttpResponse<byte[]> asset = get(path);
			assertThat(asset.statusCode()).as(path).isEqualTo(200);
			assertThat(asset.body()).as(path).isNotEmpty().containsExactly(classPathBytes("static" + path));
			assertThat(asset.headers().firstValue("Content-Type")).hasValueSatisfying(value ->
					assertThat(value).contains(path.endsWith(".css") ? "text/css" : "javascript"));
			if (path.endsWith(".js")) {
				var workerPaths = Pattern.compile("/assets/maplibre-gl-worker-[\\w-]+\\.js")
						.matcher(new String(asset.body(), StandardCharsets.UTF_8))
						.results().map(match -> match.group()).distinct().toList();
				assertThat(workerPaths).as("Отдельный обработчик геометрии входит в production bundle").hasSize(1);
				HttpResponse<byte[]> worker = get(workerPaths.getFirst());
				assertThat(worker.statusCode()).isEqualTo(200);
				assertThat(worker.headers().firstValue("Content-Type")).hasValueSatisfying(value ->
						assertThat(value).contains("javascript"));
				assertThat(worker.body()).isNotEmpty()
						.containsExactly(classPathBytes("static" + workerPaths.getFirst()));
			}
		}
	}

	@Test
	@DisplayName("Неизвестные API и файлы не подменяются страницей карты")
	void doesNotReplaceMissingResourcesWithFrontend() throws Exception {
		for (String path : new String[]{"/api/v1/unknown", "/assets/missing.js", "/unknown-page"}) {
			assertThat(get(path).statusCode()).as(path).isEqualTo(404);
		}
	}

	@Test
	@DisplayName("Отдает опубликованную пару без изменения байтов и с точными типами содержимого")
	void servesPublishedPairWithExactBytesAndMediaTypes() throws Exception {
		byte[] expectedGeoJson = classPathBytes(RESOURCE_ROOT + "/countries.geojson");
		byte[] expectedManifest = classPathBytes(RESOURCE_ROOT + "/manifest.json");

		HttpResponse<byte[]> geoJsonResponse = get(HTTP_ROOT + "/countries.geojson");
		HttpResponse<byte[]> manifestResponse = get(HTTP_ROOT + "/manifest.json");

		assertThat(geoJsonResponse.statusCode()).isEqualTo(200);
		assertThat(geoJsonResponse.headers().firstValue("Content-Type"))
				.contains(GEO_JSON_MEDIA_TYPE);
		assertThat(geoJsonResponse.body()).containsExactly(expectedGeoJson);
		assertThat(manifestResponse.statusCode()).isEqualTo(200);
		assertThat(manifestResponse.headers().firstValue("Content-Type"))
				.contains(JSON_MEDIA_TYPE);
		assertThat(manifestResponse.body()).containsExactly(expectedManifest);
	}

	@Test
	@DisplayName("Версия и контрольная сумма GeoJSON совпадают с каталогом и manifest")
	void keepsGeometryVersionAndHashConsistent() throws Exception {
		byte[] geoJsonBytes = classPathBytes(RESOURCE_ROOT + "/countries.geojson");
		byte[] manifestBytes = classPathBytes(RESOURCE_ROOT + "/manifest.json");
		var jsonMapper = JsonMapper.builder().build();
		JsonNode geoJson = jsonMapper.readTree(geoJsonBytes);
		JsonNode manifest = jsonMapper.readTree(manifestBytes);

		assertThat(geoJson.path("geometryVersion").asString()).isEqualTo(GEOMETRY_VERSION);
		assertThat(manifest.path("geometryVersion").asString()).isEqualTo(GEOMETRY_VERSION);
		assertThat(manifest.path("artifact").path("fileName").asString())
				.isEqualTo("countries.geojson");
		assertThat(manifest.path("artifact").path("bytes").asLong())
				.isEqualTo(geoJsonBytes.length);
		assertThat(manifest.path("artifact").path("sha256").asString())
				.isEqualTo(sha256(geoJsonBytes));
	}

	@Test
	@DisplayName("Неизвестная версия и адреса без точной версии возвращают 404 без подмены")
	void rejectsUnknownVersionAndUnversionedAliases() throws Exception {
		for (String path : new String[]{
				"/map/geometry/country-v999/countries.geojson",
				"/map/geometry/country-v999/manifest.json",
				"/map/geometry/countries.geojson",
				"/map/geometry/manifest.json",
				"/map/geometry/latest/countries.geojson",
				"/map/geometry/latest/manifest.json"
		}) {
			assertThat(get(path).statusCode()).as(path).isEqualTo(404);
		}
	}

	private HttpResponse<byte[]> get(String path) throws Exception {
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create("http://127.0.0.1:" + serverPort + path))
				.GET()
				.build();
		return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
	}

	private static byte[] classPathBytes(String path) throws Exception {
		return new ClassPathResource(path).getContentAsByteArray();
	}

	private static String sha256(byte[] bytes) throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration(exclude = {
			DataSourceAutoConfiguration.class,
			FlywayAutoConfiguration.class
	})
	@Import(MapGeometryStaticResourceConfiguration.class)
	static class StaticDeliveryTestApplication {
	}
}
