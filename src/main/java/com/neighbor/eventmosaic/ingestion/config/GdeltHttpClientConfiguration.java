package com.neighbor.eventmosaic.ingestion.config;

import java.net.http.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Создает общий JDK HTTP client для manifest и archive запросов GDELT.
 */
@Configuration(proxyBeanMethods = false)
public class GdeltHttpClientConfiguration {

	/**
	 * Настраивает единый client без автоматического следования redirect.
	 *
	 * @param properties сетевые настройки ingestion
	 * @return HTTP client для адаптеров GDELT
	 */
	@Bean
	public HttpClient gdeltHttpClient(GdeltIngestionProperties properties) {
		return HttpClient.newBuilder()
				.connectTimeout(properties.http().connectTimeout())
				.followRedirects(HttpClient.Redirect.NEVER)
				.build();
	}
}
