package com.neighbor.eventmosaic.ingestion.api;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.archive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.ingestion.error.ArchiveContentViolationException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Контракты загрузки")
class IngestionContractsTest {

	@Test
	@DisplayName("Обновление содержит ровно по одному архиву каждого поддерживаемого типа")
	void discoveredUpdateRequiresExactlyOneArchivePerSupportedType() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		DiscoveredArchive events = archive(
				updateTime,
				ArchiveType.TRANSLATION_EVENTS,
				"ABCDEFABCDEFABCDEFABCDEFABCDEFAB",
				1024);
		DiscoveredArchive mentions = archive(
				updateTime,
				ArchiveType.TRANSLATION_MENTIONS,
				"ABCDEFABCDEFABCDEFABCDEFABCDEFAB",
				1024);

		DiscoveredUpdate update = new DiscoveredUpdate(updateTime, List.of(events, mentions), List.of());
		List<DiscoveredArchive> duplicateEvents = List.of(events, events);
		List<DiscoveryDiagnostic> noDiagnostics = List.of();

		assertThat(update.archives()).containsExactly(events, mentions);
		assertThatThrownBy(() -> new DiscoveredUpdate(updateTime, duplicateEvents, noDiagnostics))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("exactly one Event");
	}

	@Test
	@DisplayName("Архив нормализует контрольную сумму и формирует стабильный ключ")
	void archiveNormalizesChecksumAndBuildsStableKey() {
		DiscoveredArchive archive = archive(
				Instant.parse("2026-07-20T12:00:00Z"),
				ArchiveType.TRANSLATION_EVENTS,
				"ABCDEFABCDEFABCDEFABCDEFABCDEFAB",
				1024
		);

		assertThat(archive.expectedMd5()).isEqualTo("abcdefabcdefabcdefabcdefabcdefab");
		assertThat(archive.idempotencyKey()).endsWith(":abcdefabcdefabcdefabcdefabcdefab");
	}

	@Test
	@DisplayName("Архив делегирует обязательность checksum общему source contract")
	void archiveDelegatesRequiredChecksumToSourceContract() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		String archiveName = "20260720120000.translation.export.CSV.zip";
		URI metadataUri = URI.create("http://data.gdeltproject.org/gdeltv2/" + archiveName);

		assertThatThrownBy(() -> new DiscoveredArchive(
				updateTime,
				archiveName,
				metadataUri,
				null,
				ArchiveType.TRANSLATION_EVENTS,
				1024))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("checksum must not be null");
	}

	@Test
	@DisplayName("Повторяемая ingestion-ошибка принадлежит retryable-ветке и сохраняется как повторяемая")
	void retryableFailureFollowsRetryableHierarchy() {
		IllegalStateException cause = new IllegalStateException("transport unavailable");
		RemoteSourceAccessException exception = new RemoteSourceAccessException(
				IngestionErrorCode.DOWNLOAD_TIMEOUT,
				cause);

		assertThat(RemoteSourceAccessException.class.getSuperclass())
				.isEqualTo(RetryableException.class);
		assertThat(exception.errorCode()).isEqualTo(IngestionErrorCode.DOWNLOAD_TIMEOUT);
		assertThat(exception.getMessage()).isEqualTo(IngestionErrorCode.DOWNLOAD_TIMEOUT.safeMessage());
		assertThat(exception.getCause()).isSameAs(cause);
		assertThat(exception.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.DOWNLOAD_TIMEOUT, true));
	}

	@Test
	@DisplayName("Неповторяемая ingestion-ошибка принадлежит non-retryable-ветке и блокирует повтор")
	void nonRetryableFailureFollowsNonRetryableHierarchy() {
		ArchiveContentViolationException exception = new ArchiveContentViolationException(
				IngestionErrorCode.ZIP_CONTENT_MISMATCH);

		assertThat(ArchiveContentViolationException.class.getSuperclass())
				.isEqualTo(NonRetryableException.class);
		assertThat(exception.errorCode()).isEqualTo(IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		assertThat(exception.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.ZIP_CONTENT_MISMATCH, false));
	}

	@Test
	@DisplayName("Conditional transition принимает только ноль или одну измененную строку")
	void transitionResultRejectsUnexpectedUpdatedRowCount() {
		assertThat(AttemptTransitionResult.fromUpdatedRows(0))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(AttemptTransitionResult.fromUpdatedRows(1))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThatThrownBy(() -> AttemptTransitionResult.fromUpdatedRows(2))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("unexpected number of rows");
	}

	@Test
	@DisplayName("Attempt state отклоняет retry counters без соответствующих claims")
	void attemptStateRejectsRetryCountersWithoutClaims() {
		Instant now = Instant.parse("2026-07-20T12:00:00Z");
		AutomaticRetryState impossibleAutomaticRetry =
				new AutomaticRetryState(1, 0, 3, null);
		AutomaticRetryState impossibleFailureSequence =
				new AutomaticRetryState(0, 2, 3, now);

		assertInvalidAttempt(
				() -> new ArchiveAttemptState(1, null, now, null, impossibleAutomaticRetry),
				"automaticRetriesUsed");
		assertInvalidAttempt(
				() -> new ArchiveProcessingAttemptState(
						1, null, now, null, impossibleAutomaticRetry),
				"automaticRetriesUsed");
		assertInvalidAttempt(
				() -> new SourcePollAttemptState(1, null, now, null, impossibleAutomaticRetry),
				"automaticRetriesUsed");

		assertInvalidAttempt(
				() -> new ArchiveAttemptState(1, null, now, null, impossibleFailureSequence),
				"consecutiveRetryableFailures");
		assertInvalidAttempt(
				() -> new ArchiveProcessingAttemptState(
						1, null, now, null, impossibleFailureSequence),
				"consecutiveRetryableFailures");
		assertInvalidAttempt(
				() -> new SourcePollAttemptState(1, null, now, null, impossibleFailureSequence),
				"consecutiveRetryableFailures");
	}

	private static void assertInvalidAttempt(Supplier<?> constructor, String messagePart) {
		assertThatThrownBy(constructor::get)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining(messagePart);
	}

}
