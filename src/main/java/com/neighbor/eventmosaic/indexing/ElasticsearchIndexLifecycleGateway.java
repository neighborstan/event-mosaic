package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.CommonStatsFlag;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch.indices.AddBlockResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.IndicesBlockOptions;
import co.elastic.clients.elasticsearch.indices.IndicesStatsResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettingBlocks;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.elasticsearch.indices.RemoveBlockResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesRequest;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.nodes.NodesStatsResponse;
import co.elastic.clients.elasticsearch.nodes.stats.NodeStatsMetric;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

/** Official-client adapter для exact lifecycle операций без wildcard. */
@Component
final class ElasticsearchIndexLifecycleGateway
		implements IndexLifecycleElasticsearchGateway, IndexMaintenanceGateway {

	private static final String INDEX_NOT_FOUND = "index_not_found_exception";
	private static final String INDEX_ALREADY_EXISTS = "resource_already_exists_exception";

	private final ElasticsearchClient client;
	private final ElasticsearchIndexTemplateInstaller templateInstaller;
	private final ElasticsearchArchiveReceiptVerifier receiptVerifier;

	ElasticsearchIndexLifecycleGateway(
			ElasticsearchClient client,
			ElasticsearchIndexTemplateInstaller templateInstaller
	) {
		this.client = Objects.requireNonNull(client, "client must not be null");
		this.templateInstaller = Objects.requireNonNull(
				templateInstaller, "templateInstaller must not be null");
		this.receiptVerifier = new ElasticsearchArchiveReceiptVerifier(client);
	}

	@Override
	public void installTemplates() {
		try {
			templateInstaller.install();
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
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
	public Optional<ObservedIndex> observeExactIndex(String indexName) {
		try {
			return findExactIndex(indexName).map(index -> new ObservedIndex(
					index.indexName(),
					index.indexUuid(),
					index.writeBlocked()));
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public Optional<ExactIndexAliasMembership> observeAllAliasesForExactIndex(
			String indexName
	) {
		requireExactName(indexName);
		Optional<ObservedIndex> before = observeExactIndex(indexName);
		if (before.isEmpty()) {
			return Optional.empty();
		}
		try {
			GetAliasResponse response = client.indices().getAlias(request -> request
					.index(indexName)
					.allowNoIndices(false)
					.ignoreUnavailable(false));
			var aliases = response.aliases().get(indexName);
			if (aliases == null) {
				throw protocolFailure();
			}
			Optional<ObservedIndex> after = observeExactIndex(indexName);
			if (after.isEmpty()) {
				return Optional.empty();
			}
			if (!before.orElseThrow().indexUuid().equals(after.orElseThrow().indexUuid())) {
				throw protocolFailure();
			}
			return Optional.of(new ExactIndexAliasMembership(
					indexName,
					after.orElseThrow().indexUuid(),
					aliases.aliases().keySet()));
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			if (hasType(exception, INDEX_NOT_FOUND)
					&& observeExactIndex(indexName).isEmpty()) {
				return Optional.empty();
			}
			throw classify(exception);
		}
	}

	@Override
	public long exactIndexStoreBytes(ExactIndexTarget target) {
		Objects.requireNonNull(target, "target must not be null");
		requireObservedIdentity(target);
		try {
			IndicesStatsResponse response = client.indices().stats(request -> request
					.index(target.indexName())
					.metric(CommonStatsFlag.Store));
			var stats = response.indices().get(target.indexName());
			if (response.shards().failed().longValue() > 0
					|| stats == null
					|| !target.indexUuid().equals(stats.uuid())
					|| stats.total() == null
					|| stats.total().store() == null
					|| stats.total().store().sizeInBytes() < 0) {
				throw protocolFailure();
			}
			return stats.total().store().sizeInBytes();
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public void createExactIndex(String indexName) {
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
				throw classify(exception);
			}
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
	}

	@Override
	public IndexAliasMembership readStableAliases() throws IOException {
		return new IndexAliasMembership(
				readAlias(GdeltIndexKind.EVENT.readAlias()),
				readAlias(GdeltIndexKind.MENTION.readAlias()));
	}

	@Override
	public AliasMembership readAliases() {
		try {
			IndexAliasMembership membership = readStableAliases();
			return new AliasMembership(
					membership.eventIndices(),
					membership.mentionIndices());
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
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
	) {
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
		try {
			UpdateAliasesResponse response = client.indices().updateAliases(request.build());
			if (!response.acknowledged()) {
				throw unavailable();
			}
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public void addWriteBlock(List<ExactIndexTarget> targets) {
		List<ExactIndexTarget> exactTargets = requireTargets(targets);
		if (exactTargets.isEmpty()) {
			return;
		}
		exactTargets.forEach(this::requireObservedIdentity);
		try {
			AddBlockResponse response = client.indices().addBlock(request -> request
					.index(exactTargets.stream().map(ExactIndexTarget::indexName).toList())
					.block(IndicesBlockOptions.Write)
					.allowNoIndices(false)
					.ignoreUnavailable(false));
			if (!response.acknowledged()
					|| !response.shardsAcknowledged()
					|| response.indices().size() != exactTargets.size()
					|| response.indices().stream().anyMatch(status -> !status.blocked())
					|| !Set.copyOf(response.indices().stream()
							.map(status -> status.name())
							.toList()).equals(Set.copyOf(exactTargets.stream()
							.map(ExactIndexTarget::indexName)
							.toList()))) {
				throw unavailable();
			}
			exactTargets.forEach(target -> {
				ObservedIndex observed = requireObservedIdentity(target);
				if (!observed.writeBlocked()) {
					throw unavailable();
				}
			});
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public void removeWriteBlock(List<ExactIndexTarget> targets) {
		List<ExactIndexTarget> exactTargets = requireTargets(targets);
		List<ExactIndexTarget> blocked = new ArrayList<>();
		for (ExactIndexTarget target : exactTargets) {
			Optional<ObservedIndex> observed = observeExactIndex(target.indexName());
			if (observed.isEmpty()) {
				continue;
			}
			if (!target.indexUuid().equals(observed.orElseThrow().indexUuid())) {
				throw protocolFailure();
			}
			if (observed.orElseThrow().writeBlocked()) {
				blocked.add(target);
			}
		}
		if (!blocked.isEmpty()) {
			try {
				RemoveBlockResponse response = client.indices().removeBlock(request -> request
						.index(blocked.stream().map(ExactIndexTarget::indexName).toList())
						.block(IndicesBlockOptions.Write)
						.allowNoIndices(false)
						.ignoreUnavailable(false));
				if (!response.acknowledged()
						|| response.indices().size() != blocked.size()
						|| response.indices().stream().anyMatch(status ->
								!Boolean.TRUE.equals(status.unblocked())
										|| status.exception() != null)
						|| !Set.copyOf(response.indices().stream()
								.map(status -> status.name())
								.toList()).equals(Set.copyOf(blocked.stream()
								.map(ExactIndexTarget::indexName)
								.toList()))) {
					throw unavailable();
				}
			}
			catch (IOException exception) {
				throw ioFailure(exception);
			}
			catch (ElasticsearchException exception) {
				throw classify(exception);
			}
		}
		for (ExactIndexTarget target : exactTargets) {
			Optional<ObservedIndex> observed = observeExactIndex(target.indexName());
			if (observed.isPresent()
					&& (!target.indexUuid().equals(observed.orElseThrow().indexUuid())
						|| observed.orElseThrow().writeBlocked())) {
				throw unavailable();
			}
		}
	}

	@Override
	public void cutoverAliases(AliasCutover cutover) {
		Objects.requireNonNull(cutover, "cutover must not be null");
		List.of(cutover.newEvent(), cutover.newMention())
				.forEach(this::requireObservedIdentity);
		if (cutover.oldEvent() != null) {
			requireObservedIdentity(cutover.oldEvent());
		}
		if (cutover.oldMention() != null) {
			requireObservedIdentity(cutover.oldMention());
		}
		UpdateAliasesRequest.Builder request = new UpdateAliasesRequest.Builder();
		if (cutover.oldEvent() != null) {
			request.actions(action -> action.remove(remove -> remove
					.index(cutover.oldEvent().indexName())
					.alias(GdeltIndexKind.EVENT.readAlias())
					.mustExist(true)));
		}
		if (cutover.oldMention() != null) {
			request.actions(action -> action.remove(remove -> remove
					.index(cutover.oldMention().indexName())
					.alias(GdeltIndexKind.MENTION.readAlias())
					.mustExist(true)));
		}
		request.actions(action -> action.add(add -> add
				.index(cutover.newEvent().indexName())
				.alias(GdeltIndexKind.EVENT.readAlias())));
		request.actions(action -> action.add(add -> add
				.index(cutover.newMention().indexName())
				.alias(GdeltIndexKind.MENTION.readAlias())));
		try {
			UpdateAliasesResponse response = client.indices().updateAliases(request.build());
			if (!response.acknowledged()) {
				throw unavailable();
			}
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public void deleteExactIndex(ExactIndexTarget target) {
		Objects.requireNonNull(target, "target must not be null");
		requireExactName(target.indexName());
		ExactIndexAliasMembership observed = observeAllAliasesForExactIndex(
				target.indexName()).orElseThrow(
					ElasticsearchIndexLifecycleGateway::protocolFailure);
		if (!target.indexUuid().equals(observed.indexUuid())
				|| !observed.aliases().isEmpty()) {
			throw protocolFailure();
		}
		ExactIndexAliasMembership confirmed = observeAllAliasesForExactIndex(
				target.indexName()).orElseThrow(
					ElasticsearchIndexLifecycleGateway::protocolFailure);
		if (!target.indexUuid().equals(confirmed.indexUuid())
				|| !confirmed.aliases().isEmpty()) {
			throw protocolFailure();
		}
		try {
			DeleteIndexResponse response = client.indices().delete(request -> request
					.index(target.indexName())
					.allowNoIndices(false)
					.ignoreUnavailable(false));
			if (!response.acknowledged()) {
				throw unavailable();
			}
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public long minimumAvailableDiskBytes() {
		try {
			NodesStatsResponse response = client.nodes().stats(request -> request
					.metric(NodeStatsMetric.Fs));
			if (response.nodeStats() == null
					|| response.nodeStats().failed() > 0
					|| response.nodes().isEmpty()) {
				throw unavailable();
			}
			return response.nodes().values().stream()
					.filter(stats -> stats.roles().stream().anyMatch(role ->
							role.jsonValue().equals("data")
									|| role.jsonValue().startsWith("data_")))
					.map(stats -> stats.fs() == null ? null : stats.fs().total())
					.filter(Objects::nonNull)
					.map(total -> total.availableInBytes())
					.filter(Objects::nonNull)
					.filter(value -> value >= 0)
					.mapToLong(Long::longValue)
					.min()
					.orElseThrow(ElasticsearchIndexLifecycleGateway::unavailable);
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public void refreshExact(GdeltIndexKind kind, ExactIndexTarget target) {
		Objects.requireNonNull(kind, "kind must not be null");
		if (!kind.accepts(target)) {
			throw new IllegalArgumentException("target must match kind");
		}
		requireObservedIdentity(target);
		try {
			RefreshResponse response = client.indices().refresh(request -> request
					.index(target.indexName()));
			requireSuccessfulShards(response.shards());
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
	}

	@Override
	public ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query) {
		Objects.requireNonNull(query, "query must not be null");
		requireObservedIdentity(query.target());
		try {
			return receiptVerifier.verify(query);
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
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

	private ObservedIndex requireObservedIdentity(ExactIndexTarget target) {
		Objects.requireNonNull(target, "target must not be null");
		Optional<ObservedIndex> observed = observeExactIndex(target.indexName());
		if (observed.isEmpty()
				|| !target.indexUuid().equals(observed.orElseThrow().indexUuid())) {
			throw protocolFailure();
		}
		return observed.orElseThrow();
	}

	private static List<ExactIndexTarget> requireTargets(List<ExactIndexTarget> targets) {
		List<ExactIndexTarget> exactTargets = List.copyOf(
				Objects.requireNonNull(targets, "targets must not be null"));
		if (exactTargets.stream().map(ExactIndexTarget::indexName).distinct().count()
				!= exactTargets.size()) {
			throw new IllegalArgumentException("target index names must be unique");
		}
		exactTargets.forEach(target -> requireExactName(target.indexName()));
		return exactTargets;
	}

	private static void requireSuccessfulShards(ShardStatistics shards) {
		if (shards == null || shards.failed().longValue() > 0) {
			throw unavailable();
		}
	}

	private static IndexingAccessException unavailable() {
		return new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
	}

	private static IndexingProtocolException protocolFailure() {
		return new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
	}

	private static RuntimeException ioFailure(IOException exception) {
		if (Thread.currentThread().isInterrupted()) {
			return new com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException(
					exception);
		}
		return new IndexingAccessException(
				IndexingErrorCode.INDEXING_UNAVAILABLE,
				exception);
	}

	private static RuntimeException classify(ElasticsearchException exception) {
		int status = exception.status();
		if (status == 408 || status == 429 || (status >= 500 && status <= 599)) {
			return new IndexingAccessException(
					IndexingErrorCode.INDEXING_UNAVAILABLE,
					exception);
		}
		return new IndexingProtocolException(
				IndexingErrorCode.INDEXING_REQUEST_REJECTED,
				exception);
	}

	private static void requireExactName(String indexName) {
		Objects.requireNonNull(indexName, "indexName must not be null");
		if (!indexName.matches("^gdelt-(events|mentions)-v1-p[0-9]{8}-g[0-9]{4,}$")) {
			throw new IllegalArgumentException("Lifecycle requires exact schema v1 index name");
		}
	}
}
