package com.neighbor.eventmosaic.ingestion.maintenance;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupOutcome;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildErrorCode;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildOutcome;
import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Публикует длительность rebuild и результаты cleanup без идентификаторов
 * partition, generation или оператора.
 */
@Component
final class MaintenanceMetrics {

	static final String REBUILD_DURATION_METER =
			"event_mosaic.maintenance.rebuild.duration";
	static final String CLEANUP_EXECUTIONS_METER =
			"event_mosaic.maintenance.cleanup.executions";

	private static final String OPERATION_TAG = "operation";
	private static final String OUTCOME_TAG = "outcome";
	private static final String CODE_TAG = "code";
	private static final String INSPECT_OPERATION = "inspect";
	private static final String EXECUTE_OPERATION = "execute";
	private static final String COMPLETED_OUTCOME = "completed";
	private static final String FAILED_OUTCOME = "failed";
	private static final String NO_ERROR_CODE = "none";
	private static final String UNEXPECTED_FAILURE_CODE = "unexpected_failure";

	private final MeterRegistry meterRegistry;

	MaintenanceMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(
				meterRegistry, "meterRegistry must not be null");
	}

	/** Записывает успешное завершение read-only rebuild inspect. */
	void rebuildInspectCompleted(long elapsedNanos) {
		recordRebuild(elapsedNanos, INSPECT_OPERATION, COMPLETED_OUTCOME, NO_ERROR_CODE);
	}

	/** Записывает каталогизированный отказ read-only rebuild inspect. */
	void rebuildInspectFailed(long elapsedNanos, PartitionRebuildErrorCode errorCode) {
		recordRebuild(elapsedNanos, INSPECT_OPERATION, FAILED_OUTCOME, tag(errorCode));
	}

	/** Записывает типизированный результат rebuild execute. */
	void rebuildExecuted(long elapsedNanos, PartitionRebuildOutcome outcome) {
		recordRebuild(
				elapsedNanos,
				EXECUTE_OPERATION,
				tag(Objects.requireNonNull(outcome, "outcome must not be null")),
				NO_ERROR_CODE);
	}

	/** Записывает каталогизированный отказ rebuild execute. */
	void rebuildExecuteFailed(long elapsedNanos, PartitionRebuildErrorCode errorCode) {
		recordRebuild(elapsedNanos, EXECUTE_OPERATION, FAILED_OUTCOME, tag(errorCode));
	}

	/** Записывает типизированный результат cleanup execute. */
	void cleanupExecuted(GenerationCleanupOutcome outcome) {
		recordCleanup(
				tag(Objects.requireNonNull(outcome, "outcome must not be null")),
				NO_ERROR_CODE);
	}

	/** Записывает каталогизированный отказ cleanup execute. */
	void cleanupExecuteFailed(ApplicationErrorCode errorCode) {
		Objects.requireNonNull(errorCode, "errorCode must not be null");
		recordCleanup(FAILED_OUTCOME, errorCode.code().toLowerCase(Locale.ROOT));
	}

	/** Записывает неожиданный cleanup failure без внешнего текста. */
	void cleanupExecuteUnexpectedFailure() {
		recordCleanup(FAILED_OUTCOME, UNEXPECTED_FAILURE_CODE);
	}

	private void recordRebuild(
			long elapsedNanos,
			String operation,
			String outcome,
			String code
	) {
		if (elapsedNanos < 0) {
			throw new IllegalArgumentException("elapsedNanos must not be negative");
		}
		meterRegistry.timer(
				REBUILD_DURATION_METER,
				OPERATION_TAG, operation,
				OUTCOME_TAG, outcome,
				CODE_TAG, code
		).record(elapsedNanos, TimeUnit.NANOSECONDS);
	}

	private void recordCleanup(String outcome, String code) {
		meterRegistry.counter(
				CLEANUP_EXECUTIONS_METER,
				OUTCOME_TAG, outcome,
				CODE_TAG, code
		).increment();
	}

	private static String tag(Enum<?> value) {
		return Objects.requireNonNull(value, "metric tag value must not be null")
				.name()
				.toLowerCase(Locale.ROOT);
	}
}
