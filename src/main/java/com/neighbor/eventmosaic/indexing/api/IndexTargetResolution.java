package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/** Typed result без исключений и auto-create для ожидаемых lifecycle outcomes. */
public record IndexTargetResolution(
		IndexTargetResolutionStatus status,
		ActiveIndexTargets targets
) {

	/** Проверяет наличие targets только для READY. */
	public IndexTargetResolution {
		Objects.requireNonNull(status, "status must not be null");
		if ((status == IndexTargetResolutionStatus.READY) != (targets != null)) {
			throw new IllegalArgumentException("Only READY resolution may contain exact targets");
		}
	}

	public static IndexTargetResolution ready(ActiveIndexTargets targets) {
		return new IndexTargetResolution(
				IndexTargetResolutionStatus.READY,
				Objects.requireNonNull(targets, "targets must not be null"));
	}

	public static IndexTargetResolution outcome(IndexTargetResolutionStatus status) {
		if (status == IndexTargetResolutionStatus.READY) {
			throw new IllegalArgumentException("READY requires exact targets");
		}
		return new IndexTargetResolution(status, null);
	}
}
