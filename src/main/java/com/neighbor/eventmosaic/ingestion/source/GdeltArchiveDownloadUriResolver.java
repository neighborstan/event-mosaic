package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import java.net.URI;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Разрешает archive URI только внутри официального GDELT GCS-каталога.
 */
@Component
public final class GdeltArchiveDownloadUriResolver implements ArchiveDownloadUriResolver {

	private final URI baseUri;

	/**
	 * Создает resolver из проверенных runtime properties.
	 *
	 * @param properties настройки источника GDELT
	 */
	public GdeltArchiveDownloadUriResolver(GdeltIngestionProperties properties) {
		Objects.requireNonNull(properties, "properties must not be null");
		this.baseUri = GdeltSourceContract.requireOfficialDownloadBaseUri(properties.baseUri());
	}

	@Override
	public URI resolve(GdeltArchiveName archiveName) {
		Objects.requireNonNull(archiveName, "archiveName must not be null");
		return baseUri.resolve(archiveName.value());
	}
}
