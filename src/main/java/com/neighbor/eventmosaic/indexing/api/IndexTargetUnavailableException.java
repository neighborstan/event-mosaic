package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Передает typed ownership или maintenance outcome без раскрытия exact target.
 *
 * <p>Owning processing boundary обязан перехватить этот outcome до сохранения
 * обычного archive failure и повторно разрешить current generation.</p>
 */
@SuppressWarnings("java:S110")
public final class IndexTargetUnavailableException extends NonRetryableException
		implements IndexingFailureContract {

	private final IndexTargetUnavailableReason reason;

	/**
	 * Создает bounded target outcome без transport cause.
	 *
	 * @param reason различимая ownership или maintenance причина
	 */
	public IndexTargetUnavailableException(IndexTargetUnavailableReason reason) {
		this(reason, null);
	}

	/**
	 * Создает bounded target outcome с локальной технической причиной.
	 *
	 * @param reason различимая ownership или maintenance причина
	 * @param cause локальная техническая причина
	 */
	public IndexTargetUnavailableException(
			IndexTargetUnavailableReason reason,
			Throwable cause
	) {
		super(Objects.requireNonNull(reason, "reason must not be null").errorCode(), cause);
		this.reason = reason;
	}

	/**
	 * Возвращает bounded outcome для owning boundary.
	 *
	 * @return причина недоступности exact target
	 */
	public IndexTargetUnavailableReason reason() {
		return reason;
	}

	@Override
	public boolean retryable() {
		return false;
	}
}
