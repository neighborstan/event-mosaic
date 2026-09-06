package com.neighbor.eventmosaic.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Метрики загрузки")
class IngestionMetricsTest {

	@Test
	@DisplayName("Метрики публикуют только теги с небольшим и заранее ограниченным набором значений")
	void publishesOnlyTagsWithStableBoundedValues() {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		IngestionMetrics metrics = new IngestionMetrics(registry);

		metrics.runStarted();
		metrics.runCompleted(IngestionRunStatus.PARTIAL);
		metrics.archiveOutcome(ArchiveType.TRANSLATION_EVENTS, ArchiveOutcome.STAGED);
		metrics.downloadOutcome(ArchiveType.TRANSLATION_EVENTS, DownloadOutcome.DOWNLOADED);
		metrics.cleanupFailed();
		metrics.gapsCreated(2);
		metrics.retry(ArchiveType.TRANSLATION_MENTIONS, true);
		metrics.error(IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
		metrics.event(IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE);
		metrics.cycleDuration(10, IngestionCycleOutcome.STORAGE_PRESSURE);
		metrics.acquisitionDuration(20, IngestionOperationMetricOutcome.RETRY_DEFERRED);

		assertCounter(registry, "event_mosaic.ingestion.runs", 1, "outcome", "started");
		assertCounter(registry, "event_mosaic.ingestion.runs", 1, "outcome", "partial");
		assertCounter(registry, "event_mosaic.ingestion.archives", 1,
				"type", "translation_events", "outcome", "staged");
		assertCounter(registry, "event_mosaic.ingestion.downloads", 1,
				"type", "translation_events", "outcome", "downloaded");
		assertCounter(registry, "event_mosaic.ingestion.cleanup", 1,
				"outcome", "failed");
		assertCounter(registry, "event_mosaic.ingestion.gaps", 2, "outcome", "detected");
		assertCounter(registry, "event_mosaic.ingestion.retries", 1,
				"type", "translation_mentions", "recovered", "true");
		assertCounter(registry, "event_mosaic.ingestion.errors", 1,
				"code", "download_md5_mismatch");
		assertCounter(registry, "event_mosaic.ingestion.events", 1,
				"code", "manifest_unsupported_archive");
		assertThat(registry.get("event_mosaic.ingestion.cycle.duration")
				.tag("outcome", "storage_pressure").timer().count()).isEqualTo(1);
		assertThat(registry.get("event_mosaic.ingestion.acquisition.duration")
				.tag("outcome", "retry_deferred").timer().count()).isEqualTo(1);
	}

	private static void assertCounter(
			SimpleMeterRegistry registry,
			String name,
			double expectedCount,
			String... tags
	) {
		var counter = registry.get(name).tags(tags).counter();
		assertThat(counter.count()).isEqualTo(expectedCount);
		assertThat(counter.getId().getTags())
				.containsExactlyInAnyOrderElementsOf(Tags.of(tags).stream().toList());
	}
}
