package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Передает неизменяемый снимок explicit inspect в execute. Подтверждения
 * внешних проверок не заменяют повторные проверки ledger под partition lock.
 */
public record CleanupClaim(
		CleanupCandidateSnapshot candidate,
		String planFingerprint,
		Instant planExpiresAt,
		String actor,
		String reasonCode,
		boolean aliasesConfirmedAbsent,
		boolean currentReceiptsConfirmed,
		boolean replaySourcesConfirmed
) {

	/** Проверяет plan identity и обязательные audit-поля claim. */
	public CleanupClaim {
		Objects.requireNonNull(candidate, "candidate");
		requireText(planFingerprint, "planFingerprint");
		Objects.requireNonNull(planExpiresAt, "planExpiresAt");
		requireText(actor, "actor");
		requireText(reasonCode, "reasonCode");
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым");
		}
	}
}
