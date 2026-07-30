package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/**
 * Идемпотентно устанавливает composable index templates из classpath resources.
 */
@Component
final class ElasticsearchIndexTemplateInstaller {

	private final ElasticsearchClient client;

	ElasticsearchIndexTemplateInstaller(ElasticsearchClient client) {
		this.client = Objects.requireNonNull(client, "client must not be null");
	}

	/**
	 * Последовательно переустанавливает все версионированные шаблоны.
	 *
	 * @throws IOException при транспортном отказе клиента или чтения resource
	 */
	void install() throws IOException {
		for (ElasticsearchIndexTemplateDefinition definition
				: ElasticsearchIndexTemplateDefinition.values()) {
			install(definition);
		}
	}

	private void install(ElasticsearchIndexTemplateDefinition definition) throws IOException {
		byte[] templateBody = readTemplateBody(definition);
		try (InputStream input = new ByteArrayInputStream(templateBody)) {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest.Builder()
					.name(definition.templateName())
					.withJson(input)
					.build();
			PutIndexTemplateResponse response = client.indices().putIndexTemplate(request);
			if (!response.acknowledged()) {
				throw new IndexingAccessException(
						IndexingErrorCode.INDEXING_UNAVAILABLE);
			}
		}
	}

	private static byte[] readTemplateBody(
			ElasticsearchIndexTemplateDefinition definition
	) {
		try {
			return new ClassPathResource(definition.resourcePath()).getContentAsByteArray();
		}
		catch (IOException exception) {
			throw new IndexingProtocolException(
					IndexingErrorCode.INDEXING_RESPONSE_INVALID,
					exception);
		}
	}

}
