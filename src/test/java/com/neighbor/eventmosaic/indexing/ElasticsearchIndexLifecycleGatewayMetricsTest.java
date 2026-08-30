package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Метрики приемочной проверки через lifecycle gateway")
class ElasticsearchIndexLifecycleGatewayMetricsTest {

	private static final String RECEIPT_DURATION_METER =
			"event_mosaic.indexing.receipt.duration";
	private static final ExactIndexTarget TARGET = new ExactIndexTarget(
			"gdelt-events-v1-p20260727-g0001",
			"event-index-uuid");

	@Test
	@DisplayName("Ошибка проверки exact target один раз завершает receipt до чтения документов")
	void measuresReceiptPreCheckFailureOnce() throws IOException {
		ElasticsearchRequestExecutor requestExecutor =
				mock(ElasticsearchRequestExecutor.class);
		SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
		doReturn(GetIndexResponse.of(response -> response.indices(Map.of())))
				.when(requestExecutor).execute(any());
		ElasticsearchIndexLifecycleGateway gateway =
				new ElasticsearchIndexLifecycleGateway(
						new ElasticsearchIndexTemplateInstaller(requestExecutor),
						new IndexingMetrics(meterRegistry),
						requestExecutor);
		ArchiveIdentityDigest digest = ArchiveIdentityDigest.accumulator().finish();
		ArchiveReceiptQuery query = new ArchiveReceiptQuery(
				GdeltIndexKind.EVENT,
				TARGET,
				"event-archive",
				"a".repeat(64),
				0,
				digest,
				500);

		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> gateway.verifyReceipt(query))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IndexingErrorCode.INDEXING_RESPONSE_INVALID));

		Timer timer = meterRegistry.find(RECEIPT_DURATION_METER)
				.tag("kind", "event")
				.tag("outcome", "non_retryable_failure")
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(meterRegistry.find(RECEIPT_DURATION_METER).timers()).hasSize(1);
		verify(requestExecutor, times(1)).execute(any());
	}
}
