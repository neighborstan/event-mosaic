package com.neighbor.eventmosaic.ingestion.api;

import java.util.Arrays;
import java.util.Objects;

/**
 * Распознает явный аргумент серверной команды очистки одной технической
 * generation. Он нужен точке запуска приложения, чтобы отключить HTTP-сервер
 * до создания Spring context.
 */
public final class GenerationCleanupCommandLine {

	/** Общий prefix параметров команды очистки generation. */
	public static final String PROPERTY_PREFIX =
			"event-mosaic.maintenance.generation-cleanup";

	private static final String MODE_ARGUMENT = "--" + PROPERTY_PREFIX + ".mode";

	private GenerationCleanupCommandLine() {
	}

	/**
	 * Проверяет, что cleanup явно запрошен аргументом командной строки, а не
	 * скрытой настройкой окружения.
	 *
	 * @param arguments исходные аргументы запуска приложения
	 * @return {@code true}, если среди аргументов есть option режима cleanup
	 */
	public static boolean isRequested(String[] arguments) {
		Objects.requireNonNull(arguments, "arguments must not be null");
		return Arrays.stream(arguments)
				.anyMatch(argument -> MODE_ARGUMENT.equals(argument)
						|| argument.startsWith(MODE_ARGUMENT + "="));
	}
}
