package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Полный анализ Elasticsearch bulk response")
class BulkResponseAnalyzerTest {

	@Test
	@DisplayName("Постоянный отказ делает весь partial outcome неповторяемым")
	void permanentItemMakesWholePartialOutcomeNonRetryable() {
		BulkIndexCommand<IndexedEventDocument> command = command(10, 11, 12);

		var result = BulkResponseAnalyzer.analyze(
				command,
				List.of(
						item(201, false),
						item(429, true),
						item(400, true)));

		assertThat(result.kind()).isEqualTo(GdeltIndexKind.EVENT);
		assertThat(result.submitted()).isEqualTo(3);
		assertThat(result.succeeded()).isEqualTo(1);
		assertThat(result.failed()).isEqualTo(2);
		assertThat(result.firstFailedLineNumber()).isEqualTo(11);
		assertThat(result.outcome()).isEqualTo(BulkIndexOutcome.NON_RETRYABLE_PARTIAL_FAILURE);
		assertThat(result.retryable()).isFalse();
	}

	@Test
	@DisplayName("408, 429 и 5xx остаются повторяемыми после анализа всех items")
	void transientItemsRemainRetryable() {
		BulkIndexCommand<IndexedEventDocument> command = command(20, 21, 22);

		var result = BulkResponseAnalyzer.analyze(
				command,
				List.of(
						item(408, true),
						item(429, true),
						item(503, true)));

		assertThat(result.succeeded()).isZero();
		assertThat(result.failed()).isEqualTo(3);
		assertThat(result.firstFailedLineNumber()).isEqualTo(20);
		assertThat(result.outcome()).isEqualTo(BulkIndexOutcome.RETRYABLE_PARTIAL_FAILURE);
		assertThat(result.retryable()).isTrue();
	}

	@Test
	@DisplayName("Несовпадение числа items отклоняется как нарушение response contract")
	void rejectsIncompleteResponse() {
		BulkIndexCommand<IndexedEventDocument> command = command(30, 31);
		List<BulkResponseItem> responseItems = List.of(item(200, false));

		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> BulkResponseAnalyzer.analyze(
						command,
						responseItems))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
					assertThat(exception.retryable()).isFalse();
				});
	}

	private static BulkIndexCommand<IndexedEventDocument> command(long... lines) {
		List<IndexedEventDocument> documents = java.util.Arrays.stream(lines)
				.mapToObj(line -> event(line, line))
				.toList();
		return new BulkIndexCommand<>(GdeltIndexKind.EVENT, documents);
	}

	private static BulkResponseItem item(int status, boolean failed) {
		return BulkResponseItem.of(builder -> {
			builder.operationType(OperationType.Index)
					.index(GdeltIndexKind.EVENT.indexName())
					.id("id-" + status)
					.status(status);
			if (failed) {
				builder.error(error -> error
						.type("remote_error_type")
						.reason("remote reason must not escape"));
			}
			return builder;
		});
	}

	private static IndexedEventDocument event(long globalEventId, long line) {
		return new IndexedEventDocument(
				globalEventId,
				LocalDate.of(2026, 7, 30),
				Instant.parse("2026-07-30T10:00:00Z"),
				"ACT1",
				"Actor 1",
				"US",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"ACT2",
				"Actor 2",
				"RS",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				1,
				"010",
				"01",
				"0",
				1,
				1.5,
				-2.0,
				3,
				2,
				2,
				null,
				"https://example.test/event/" + globalEventId,
				Instant.parse("2026-07-30T10:15:00Z"),
				"20260730101500.translation.gkg.csv.zip",
				line,
				"a".repeat(64));
	}

}
