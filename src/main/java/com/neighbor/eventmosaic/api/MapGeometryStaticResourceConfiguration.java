package com.neighbor.eventmosaic.api;

import org.springframework.boot.web.server.MimeMappings;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.servlet.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Настраивает тип содержимого для готовой геометрии карты.
 *
 * <p>Spring продолжает отдавать файлы как обычные статические ресурсы, а эта
 * конфигурация только добавляет стандартный тип {@code application/geo+json}
 * для расширения {@code .geojson}.
 */
@Configuration(proxyBeanMethods = false)
public class MapGeometryStaticResourceConfiguration {

	private static final String GEO_JSON_EXTENSION = "geojson";
	private static final String GEO_JSON_MEDIA_TYPE = "application/geo+json";

	@Bean
	WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> mapGeometryMimeMappings() {
		return factory -> {
			var mappings = new MimeMappings();
			mappings.add(GEO_JSON_EXTENSION, GEO_JSON_MEDIA_TYPE);
			factory.addMimeMappings(mappings);
		};
	}
}
