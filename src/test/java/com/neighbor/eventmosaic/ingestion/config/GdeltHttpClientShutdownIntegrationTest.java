package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@DisplayName("Остановка HTTP-клиента GDELT вместе со Spring")
class GdeltHttpClientShutdownIntegrationTest {

	@TempDir
	Path tempDir;

	@Test
	@DisplayName("Остановка Spring отменяет незавершенный ответ без ожидания полного тела")
	void contextCloseCancelsOpenResponse() throws Exception {
		var release = new CountDownLatch(1);
		var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/open", exchange -> {
			exchange.sendResponseHeaders(200, 1000);
			exchange.getResponseBody().write('x');
			exchange.getResponseBody().flush();
			try {
				release.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			} finally {
				exchange.close();
			}
		});
		server.start();
		var context = new AnnotationConfigApplicationContext();
		context.registerBean(GdeltIngestionProperties.class, () -> GdeltTestFixtures.properties(tempDir, 1024));
		context.register(GdeltHttpClientConfiguration.class);
		context.refresh();
		HttpClient client = context.getBean(HttpClient.class);
		try (var executor = Executors.newSingleThreadExecutor()) {
			var response = client.sendAsync(HttpRequest.newBuilder(URI.create(
					"http://127.0.0.1:" + server.getAddress().getPort() + "/open")).GET().build(),
					HttpResponse.BodyHandlers.ofInputStream()).get(3, TimeUnit.SECONDS);
			try (var body = response.body()) {
				assertThat(body.read()).isEqualTo('x');
				var closed = executor.submit(context::close);
				try {
					closed.get(3, TimeUnit.SECONDS);
					assertThat(client.awaitTermination(Duration.ofSeconds(3))).isTrue();
				} finally {
					client.shutdownNow();
					closed.get(3, TimeUnit.SECONDS);
				}
			}
		} finally {
			release.countDown();
			client.shutdownNow();
			context.close();
			server.stop(0);
		}
	}
}
