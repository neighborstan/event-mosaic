package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
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

	private final ElasticsearchRequestExecutor requestExecutor;

	ElasticsearchIndexTemplateInstaller(ElasticsearchRequestExecutor requestExecutor) {
		this.requestExecutor = Objects.requireNonNull(
				requestExecutor, "requestExecutor must not be null");
	}

	/**
	 * Последовательно переустанавливает все версионированные шаблоны.
	 *
	 * @throws IOException при транспортном отказе клиента или чтения resource
	 */
	void install() throws IOException {
		install(ElasticsearchRequestContext.standalone());
	}

	/**
	 * Переустанавливает шаблоны в пределах общего ingestion cycle.
	 *
	 * @param budget общий deadline и ownership guard cycle
	 * @throws IOException при транспортном отказе клиента или чтения resource
	 */
	void install(OperationBudget budget) throws IOException {
		install(ElasticsearchRequestContext.guarded(budget));
	}

	void install(ElasticsearchRequestContext context) throws IOException {
		for (ElasticsearchIndexTemplateDefinition definition
				: ElasticsearchIndexTemplateDefinition.values()) {
			install(definition, context);
		}
	}

	private void install(
			ElasticsearchIndexTemplateDefinition definition,
			ElasticsearchRequestContext context
	) throws IOException {
		byte[] templateBody = readTemplateBody(definition);
		try (InputStream input = new ByteArrayInputStream(templateBody)) {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest.Builder()
					.name(definition.templateName())
					.withJson(input)
					.build();
			PutIndexTemplateResponse response = context.execute(
					requestExecutor,
					client -> client.indices().putIndexTemplate(request));
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
