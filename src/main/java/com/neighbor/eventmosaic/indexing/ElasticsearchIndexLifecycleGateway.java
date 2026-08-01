package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettingBlocks;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

/** Official-client adapter для exact lifecycle операций без wildcard и delete. */
@Component
final class ElasticsearchIndexLifecycleGateway implements IndexLifecycleElasticsearchGateway {

	private static final String INDEX_NOT_FOUND = "index_not_found_exception";
	private static final String INDEX_ALREADY_EXISTS = "resource_already_exists_exception";

	private final ElasticsearchClient client;
	private final ElasticsearchIndexTemplateInstaller templateInstaller;

	ElasticsearchIndexLifecycleGateway(
			ElasticsearchClient client,
			ElasticsearchIndexTemplateInstaller templateInstaller
	) {
		this.client = Objects.requireNonNull(client, "client must not be null");
		this.templateInstaller = Objects.requireNonNull(
				templateInstaller, "templateInstaller must not be null");
	}

	@Override
	public void installTemplates() throws IOException {
		templateInstaller.install();
	}

	@Override
	public Optional<ObservedElasticsearchIndex> findExactIndex(String indexName)
			throws IOException {
		requireExactName(indexName);
		try {
			IndexState state = client.indices()
					.get(request -> request
							.index(indexName)
							.allowNoIndices(true)
							.ignoreUnavailable(true))
					.get(indexName);
			if (state == null) {
				return Optional.empty();
			}
			IndexSettings settings = exactSettings(state);
			return Optional.of(new ObservedElasticsearchIndex(
					indexName,
					settings.uuid(),
					isWriteBlocked(settings.blocks())));
		}
		catch (ElasticsearchException exception) {
			if (hasType(exception, INDEX_NOT_FOUND)) {
				return Optional.empty();
			}
			throw exception;
		}
	}

	@Override
	public void createExactIndex(String indexName) throws IOException {
		requireExactName(indexName);
		try {
			CreateIndexResponse response = client.indices()
					.create(request -> request.index(indexName));
			if (!response.acknowledged() || !response.shardsAcknowledged()) {
				throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
			}
			if (!indexName.equals(response.index())) {
				throw new IndexingProtocolException(
						IndexingErrorCode.INDEXING_RESPONSE_INVALID);
			}
		}
		catch (ElasticsearchException exception) {
			if (!hasType(exception, INDEX_ALREADY_EXISTS)) {
				throw exception;
			}
		}
	}

	@Override
	public IndexAliasMembership readStableAliases() throws IOException {
		return new IndexAliasMembership(
				readAlias(GdeltIndexKind.EVENT.readAlias()),
				readAlias(GdeltIndexKind.MENTION.readAlias()));
	}

	private Set<String> readAlias(String aliasName) throws IOException {
		try {
			GetAliasResponse response = client.indices().getAlias(request -> request
					.name(aliasName)
					.allowNoIndices(true)
					.ignoreUnavailable(true));
			Set<String> indices = new HashSet<>();
			response.aliases().forEach((indexName, aliases) -> {
				if (aliases.aliases().containsKey(aliasName)) {
					indices.add(indexName);
				}
			});
			return Set.copyOf(indices);
		}
		catch (ElasticsearchException exception) {
			if (exception.status() == 404 || hasType(exception, INDEX_NOT_FOUND)) {
				return Set.of();
			}
			throw exception;
		}
	}

	@Override
	public void addStableAliases(
			String eventIndexName,
			boolean addEvent,
			String mentionIndexName,
			boolean addMention
	) throws IOException {
		requireExactName(eventIndexName);
		requireExactName(mentionIndexName);
		if (!addEvent && !addMention) {
			return;
		}
		UpdateAliasesRequest.Builder request = new UpdateAliasesRequest.Builder();
		if (addEvent) {
			request.actions(action -> action.add(add -> add
					.index(eventIndexName)
					.alias(GdeltIndexKind.EVENT.readAlias())));
		}
		if (addMention) {
			request.actions(action -> action.add(add -> add
					.index(mentionIndexName)
					.alias(GdeltIndexKind.MENTION.readAlias())));
		}
		UpdateAliasesResponse response = client.indices().updateAliases(request.build());
		if (!response.acknowledged()) {
			throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		}
	}

	private static IndexSettings exactSettings(IndexState state) {
		IndexSettings settings = state.settings();
		if (settings == null) {
			throw new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
		}
		IndexSettings nested = settings.index();
		IndexSettings exact = nested == null ? settings : nested;
		if (exact.uuid() == null || exact.uuid().isBlank()) {
			throw new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
		}
		return exact;
	}

	private static boolean isWriteBlocked(IndexSettingBlocks blocks) {
		return blocks != null && (Boolean.TRUE.equals(blocks.write())
				|| Boolean.TRUE.equals(blocks.readOnly())
				|| Boolean.TRUE.equals(blocks.readOnlyAllowDelete()));
	}

	private static boolean hasType(ElasticsearchException exception, String type) {
		return exception.error() != null && type.equals(exception.error().type());
	}

	private static void requireExactName(String indexName) {
		Objects.requireNonNull(indexName, "indexName must not be null");
		if (!indexName.matches("^gdelt-(events|mentions)-v1-p[0-9]{8}-g[0-9]{4,}$")) {
			throw new IllegalArgumentException("Lifecycle requires exact schema v1 index name");
		}
	}
}
