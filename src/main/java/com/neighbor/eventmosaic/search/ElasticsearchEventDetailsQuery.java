package com.neighbor.eventmosaic.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonpMappingException;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.search.api.EventDetails;
import com.neighbor.eventmosaic.search.api.EventDetailsQuery;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * Читает минимальные Event details из версионированных индексов Elasticsearch.
 */
@Component
public class ElasticsearchEventDetailsQuery implements EventDetailsQuery {

	private static final int NOT_FOUND_STATUS = 404;
	private static final String INDEX_NOT_FOUND_ERROR_TYPE =
			"index_not_found_exception";
	private static final List<String> MENTION_SOURCE_FIELDS = List.of(
			"mentionSourceName",
			"mentionIdentifier",
			"mentionTimeDate",
			"mentionDocTone");

	private final ElasticsearchClient elasticsearchClient;
	private final EventSearchProperties properties;

	/**
	 * Создает поисковый adapter поверх официального Elasticsearch Java Client.
	 *
	 * @param elasticsearchClient Elasticsearch client
	 * @param properties проверенные ограничения details query
	 */
	public ElasticsearchEventDetailsQuery(
			ElasticsearchClient elasticsearchClient,
			EventSearchProperties properties
	) {
		this.elasticsearchClient = elasticsearchClient;
		this.properties = properties;
	}

	@Override
	public Optional<EventDetails> findById(long eventId) {
		if (eventId <= 0) {
			throw new IllegalArgumentException("eventId must be positive");
		}
		try {
			GetRequest request = GetRequest.of(builder -> builder
					.index(GdeltIndexKind.EVENT.indexName())
					.id(Long.toString(eventId)));
			GetResponse<IndexedEventDocument> response = elasticsearchClient.get(
					request,
					IndexedEventDocument.class);
			if (!response.found()) {
				return Optional.empty();
			}
			IndexedEventDocument event = requireEventSource(response.source());
			return findSources(eventId)
					.map(sources -> toDetails(event, sources));
		} catch (ElasticsearchException exception) {
			if (isIndexNotFound(exception)) {
				return Optional.empty();
			}
			throw new SearchAccessException(exception);
		} catch (IOException | JsonpMappingException exception) {
			throw new SearchAccessException(exception);
		}
	}

	private Optional<List<MentionSourceProjection>> findSources(long eventId)
			throws IOException {
		try {
			SearchRequest request = SearchRequest.of(builder -> builder
					.index(GdeltIndexKind.MENTION.indexName())
					.size(properties.maxSourcesPerEvent())
					.allowPartialSearchResults(false)
					.source(source -> source.filter(filter -> filter
							.includes(MENTION_SOURCE_FIELDS)))
					.trackTotalHits(track -> track.enabled(false))
					.query(query -> query.term(term -> term
							.field("globalEventId")
							.value(eventId)))
					.collapse(collapse -> collapse
							.field("sourceDocumentKey"))
					.sort(sort -> sort.field(field -> field
							.field("mentionTimeDate")
							.order(SortOrder.Desc)))
					.sort(sort -> sort.field(field -> field
							.field("rawMentionId")
							.order(SortOrder.Asc))));
			SearchResponse<MentionSourceProjection> response =
					elasticsearchClient.search(
							request,
							MentionSourceProjection.class);
			requireComplete(response);
			List<MentionSourceProjection> sources = response.hits().hits().stream()
					.map(hit -> requireMentionSource(hit.source()))
					.toList();
			return Optional.of(sources);
		} catch (ElasticsearchException exception) {
			if (isIndexNotFound(exception)) {
				return Optional.empty();
			}
			throw exception;
		}
	}

	private static boolean isIndexNotFound(ElasticsearchException exception) {
		return exception.status() == NOT_FOUND_STATUS
				&& exception.error() != null
				&& INDEX_NOT_FOUND_ERROR_TYPE.equals(exception.error().type());
	}

	private static IndexedEventDocument requireEventSource(
			IndexedEventDocument source
	) {
		if (source == null) {
			throw invalidResponse("Found Elasticsearch event response has no source");
		}
		return source;
	}

	private static MentionSourceProjection requireMentionSource(
			MentionSourceProjection source
	) {
		if (source == null) {
			throw invalidResponse("Elasticsearch mention hit has no source");
		}
		return source;
	}

	private static SearchAccessException invalidResponse(String message) {
		return new SearchAccessException(new IllegalStateException(message));
	}

	private static void requireComplete(
			SearchResponse<MentionSourceProjection> response
	) {
		ShardStatistics shards = response.shards();
		if (response.timedOut() || shards.failed().longValue() > 0) {
			throw invalidResponse("Elasticsearch mention search response is partial");
		}
	}

	private static EventDetails toDetails(
			IndexedEventDocument event,
			List<MentionSourceProjection> mentions
	) {
		return new EventDetails(
				new EventDetails.Event(
						event.globalEventId(),
						event.eventDay(),
						event.dateAdded()),
				new EventDetails.Actors(
						new EventDetails.Actor(event.actor1Code(), event.actor1Name()),
						new EventDetails.Actor(event.actor2Code(), event.actor2Name())),
				new EventDetails.Classification(
						event.eventCode(),
						event.eventRootCode(),
						event.quadClass(),
						event.goldsteinScale(),
						event.averageTone()),
				toLocation(event.location()),
				mentions.stream()
						.map(ElasticsearchEventDetailsQuery::toSourceDocument)
						.toList());
	}

	private static EventDetails.Location toLocation(IndexedEventLocation location) {
		if (location == null) {
			return null;
		}
		return new EventDetails.Location(
				location.role().name(),
				location.point().lat(),
				location.point().lon(),
				location.name(),
				location.countryCode(),
				location.featureId());
	}

	private static EventDetails.SourceDocument toSourceDocument(
			MentionSourceProjection mention
	) {
		return new EventDetails.SourceDocument(
				mention.mentionSourceName(),
				mention.mentionIdentifier(),
				mention.mentionTimeDate(),
				mention.mentionDocTone());
	}
}
