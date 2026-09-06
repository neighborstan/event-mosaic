package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;

/**
 * Неизменяемая bounded-проекция состояния backend data pipeline для health и
 * метрик. Проекция не содержит identities, внешнего текста и технических причин.
 */
public record BackendDataOperationalSnapshot(
		boolean databaseAvailable,
		long lagSeconds,
		long openGaps,
		RetryCounts sourcePollRetries,
		RetryCounts acquisitionRetries,
		RetryCounts processingRetries,
		long permanentFailures,
		ReceiptCounts receipts,
		GenerationCounts generations,
		long repairRequiredPartitions,
		long openMaintenanceOperations,
		AliasConsistencyState aliasConsistency,
		LiveCounts live
) {

	/** Проверяет обязательные bounded-группы operational state. */
	public BackendDataOperationalSnapshot {
		Objects.requireNonNull(sourcePollRetries, "sourcePollRetries must not be null");
		Objects.requireNonNull(acquisitionRetries, "acquisitionRetries must not be null");
		Objects.requireNonNull(processingRetries, "processingRetries must not be null");
		Objects.requireNonNull(receipts, "receipts must not be null");
		Objects.requireNonNull(generations, "generations must not be null");
		Objects.requireNonNull(aliasConsistency, "aliasConsistency must not be null");
		Objects.requireNonNull(live, "live must not be null");
		if (lagSeconds < 0
				|| openGaps < 0
				|| permanentFailures < 0
				|| repairRequiredPartitions < 0
				|| openMaintenanceOperations < 0) {
			throw new IllegalArgumentException("operational counts must not be negative");
		}
	}

	/** Возвращает безопасное состояние, когда PostgreSQL snapshot недоступен. */
	public static BackendDataOperationalSnapshot unavailable() {
		return new BackendDataOperationalSnapshot(
				false,
				0,
				0,
				RetryCounts.empty(),
				RetryCounts.empty(),
				RetryCounts.empty(),
				0,
				ReceiptCounts.empty(),
				GenerationCounts.empty(),
				0,
				0,
				AliasConsistencyState.UNAVAILABLE,
				LiveCounts.empty());
	}

	/** Возвращает число current retry states, для которых уже наступил срок. */
	public long retryDue() {
		return sourcePollRetries.due()
				+ acquisitionRetries.due()
				+ processingRetries.due()
				+ live.receiptAuditRetries().due();
	}

	/** Возвращает число current retry states с еще не наступившим сроком. */
	public long retryDeferred() {
		return sourcePollRetries.deferred()
				+ acquisitionRetries.deferred()
				+ processingRetries.deferred()
				+ live.receiptAuditRetries().deferred();
	}

	/** Возвращает число исчерпанных серий, включая ожидающие паузу и уже готовые начать следующую серию. */
	public long retryExhausted() {
		return sourcePollRetries.exhausted()
				+ acquisitionRetries.exhausted()
				+ processingRetries.exhausted()
				+ live.receiptAuditRetries().exhausted();
	}

	/** Возраст опроса и данных источника, пауза повторов и оставшаяся работа суточного восстановления. */
	public record LiveCounts(
			long successfulPollAgeSeconds,
			long sourceOutageAgeSeconds,
			long sourceLagSeconds,
			long sourceRetryDelaySeconds,
			boolean sourceCooldown,
			boolean catalogPending,
			long eventBootstrapRemaining,
			long mentionBootstrapRemaining,
			RetryCounts receiptAuditRetries
	) {

		/** Проверяет безопасные возраста и количества; -1 означает отсутствие успешного опроса или данных. */
		public LiveCounts {
			Objects.requireNonNull(receiptAuditRetries, "receiptAuditRetries must not be null");
			if (successfulPollAgeSeconds < -1 || sourceOutageAgeSeconds < 0 || sourceLagSeconds < -1
					|| sourceRetryDelaySeconds < 0 || eventBootstrapRemaining < 0 || mentionBootstrapRemaining < 0) {
				throw new IllegalArgumentException("live counts contain invalid age or count");
			}
		}

		/** Возвращает отсутствие наблюдений источника и активного плана. */
		public static LiveCounts empty() {
			return new LiveCounts(-1, 0, -1, 0, false, false, 0, 0, RetryCounts.empty());
		}
	}

	/** Количество повторных попыток одной части загрузки по текущему состоянию. */
	public record RetryCounts(long due, long deferred, long exhausted) {

		/** Проверяет, что агрегированные counts неотрицательны. */
		public RetryCounts {
			if (due < 0 || deferred < 0 || exhausted < 0) {
				throw new IllegalArgumentException("retry counts must not be negative");
			}
		}

		private static RetryCounts empty() {
			return new RetryCounts(0, 0, 0);
		}
	}

	/** Current receipt mismatch и surplus counts. */
	public record ReceiptCounts(long mismatch, long surplus) {

		/** Проверяет, что агрегированные counts неотрицательны. */
		public ReceiptCounts {
			if (mismatch < 0 || surplus < 0) {
				throw new IllegalArgumentException("receipt counts must not be negative");
			}
		}

		private static ReceiptCounts empty() {
			return new ReceiptCounts(0, 0);
		}
	}

	/** Current BUILDING, ACTIVE и SUPERSEDED generation counts. */
	public record GenerationCounts(long building, long active, long superseded) {

		/** Проверяет, что агрегированные counts неотрицательны. */
		public GenerationCounts {
			if (building < 0 || active < 0 || superseded < 0) {
				throw new IllegalArgumentException("generation counts must not be negative");
			}
		}

		private static GenerationCounts empty() {
			return new GenerationCounts(0, 0, 0);
		}
	}
}
