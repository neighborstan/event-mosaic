package com.neighbor.eventmosaic.processing.api;

import java.util.Objects;

/**
 * Передает attempt-local техническую причину owning orchestration boundary.
 *
 * <p>Diagnostic не входит в durable result и не должен публиковаться без
 * безопасной redacted projection.</p>
 */
@FunctionalInterface
public interface ArchiveProcessingDiagnosticListener {

	/**
	 * Принимает первую ожидаемую техническую причину failed result или
	 * cleanup failure, сохраненный internal control signal.
	 *
	 * @param diagnosticFailure исходное module exception или internal signal с
	 * suppressed cleanup failure
	 */
	void onFailure(RuntimeException diagnosticFailure);

	/**
	 * Возвращает listener для вызова processor вне owning orchestration.
	 *
	 * @return listener, который намеренно не сохраняет diagnostic
	 */
	static ArchiveProcessingDiagnosticListener ignoring() {
		return diagnosticFailure ->
				Objects.requireNonNull(
						diagnosticFailure,
						"diagnosticFailure must not be null");
	}
}
