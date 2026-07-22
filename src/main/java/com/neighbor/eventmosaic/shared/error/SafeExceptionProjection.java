package com.neighbor.eventmosaic.shared.error;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

/**
 * Строит диагностическую копию Throwable без исходных сообщений и данных.
 */
public final class SafeExceptionProjection {

	private static final int MAX_DEPTH = 8;

	private SafeExceptionProjection() {
	}

	/**
	 * Копирует stack, cause chain и suppressed failures, заменяя все сообщения
	 * безопасным catalog message и именами технических типов.
	 *
	 * @param source исходная ошибка
	 * @param safeMessage разрешенное сообщение верхнего уровня
	 * @return безопасная диагностическая проекция
	 */
	public static RuntimeException from(Throwable source, String safeMessage) {
		Objects.requireNonNull(source, "source must not be null");
		Objects.requireNonNull(safeMessage, "safeMessage must not be null");
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		return from(source, safeMessage, visited, 0);
	}

	private static RuntimeException from(
			Throwable source,
			String safeMessage,
			Set<Throwable> visited,
			int depth
	) {
		String type = source.getClass().getSimpleName();
		RuntimeException safe = new RuntimeException(depth == 0
				? safeMessage + " [" + type + "]"
				: "Technical failure details were redacted [" + type + "]");
		safe.setStackTrace(source.getStackTrace());
		if (depth >= MAX_DEPTH || !visited.add(source)) {
			return safe;
		}
		if (source.getCause() != null && !visited.contains(source.getCause())) {
			safe.initCause(from(source.getCause(), safeMessage, visited, depth + 1));
		}
		for (Throwable suppressed : source.getSuppressed()) {
			if (!visited.contains(suppressed)) {
				safe.addSuppressed(from(suppressed, safeMessage, visited, depth + 1));
			}
		}
		return safe;
	}
}
