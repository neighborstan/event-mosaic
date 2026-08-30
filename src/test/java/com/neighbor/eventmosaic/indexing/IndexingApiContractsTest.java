package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProperties;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Контракты конфигурации и ошибок indexing")
class IndexingApiContractsTest {

	@Test
	@DisplayName("Bulk count и byte size принимают только безопасные диапазоны")
	void validatesBulkLimits() {
		IndexingProperties properties = new IndexingProperties(
				500,
				IndexingProperties.DEFAULT_MAX_BULK_BYTES);

		assertThat(properties.bulkSize()).isEqualTo(500);
		assertThat(properties.maxBulkBytes()).isEqualTo(5L * 1024 * 1024);
		assertThat(properties.requestTimeout()).isEqualTo(Duration.ofMinutes(2));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new IndexingProperties(
						0,
						IndexingProperties.DEFAULT_MAX_BULK_BYTES));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new IndexingProperties(
						10_001,
						IndexingProperties.DEFAULT_MAX_BULK_BYTES));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new IndexingProperties(500, 0));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new IndexingProperties(
						500,
						IndexingProperties.MAX_BULK_BYTES + 1));
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new IndexingProperties(
						500,
						IndexingProperties.DEFAULT_MAX_BULK_BYTES,
						Duration.ZERO));
	}

	@Test
	@DisplayName("Смысловые ошибки выбирают retry-ветку без публикации remote reason")
	void keepsRemoteReasonOutOfSafeMessage() {
		IndexingAccessException retryable = new IndexingAccessException(
				IndexingErrorCode.INDEXING_UNAVAILABLE,
				new IOException("remote identifier and reason"));
		IndexingProtocolException permanent = new IndexingProtocolException(
				IndexingErrorCode.INDEXING_REQUEST_REJECTED);
		IndexingInterruptedException interruption =
				new IndexingInterruptedException(new IOException("interrupted remote call"));

		assertThat(retryable).isInstanceOf(RetryableException.class);
		assertThat(retryable.retryable()).isTrue();
		assertThat(retryable.getMessage())
				.isEqualTo("Elasticsearch временно недоступен")
				.doesNotContain("remote");
		assertThat(permanent).isInstanceOf(NonRetryableException.class);
		assertThat(permanent.retryable()).isFalse();
		assertThat(interruption).isInstanceOf(RetryableException.class);
		assertThat(interruption.interruptsProcessing()).isTrue();
		assertThat(interruption.errorCode())
				.isEqualTo(IndexingErrorCode.INDEXING_INTERRUPTED);
	}

	@Test
	@DisplayName("Target outcome различает bounded ownership и maintenance причины")
	void exposesBoundedTargetUnavailableReason() {
		IndexTargetUnavailableException missing = new IndexTargetUnavailableException(
				IndexTargetUnavailableReason.MISSING,
				new IOException("gdelt-events-v1-p20260727-g0001 secret reason"));
		IndexTargetUnavailableException blocked = new IndexTargetUnavailableException(
				IndexTargetUnavailableReason.WRITE_BLOCKED);

		assertThat(missing).isInstanceOf(NonRetryableException.class);
		assertThat(missing.reason()).isEqualTo(IndexTargetUnavailableReason.MISSING);
		assertThat(missing.errorCode()).isEqualTo(IndexingErrorCode.INDEX_TARGET_MISSING);
		assertThat(missing.retryable()).isFalse();
		assertThat(missing.getMessage()).doesNotContain("gdelt-events", "secret");
		assertThat(blocked.reason())
				.isEqualTo(IndexTargetUnavailableReason.WRITE_BLOCKED);
		assertThat(blocked.errorCode())
				.isEqualTo(IndexingErrorCode.INDEX_TARGET_WRITE_BLOCKED);
	}

}
