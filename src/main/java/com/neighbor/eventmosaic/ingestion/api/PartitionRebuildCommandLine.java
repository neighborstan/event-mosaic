package com.neighbor.eventmosaic.ingestion.api;

import java.util.Arrays;
import java.util.Objects;

/**
 * Описывает явный аргумент, который переводит единое приложение в локальный
 * режим восстановления partition. Этот узкий контракт нужен composition root,
 * чтобы отключить HTTP-сервер до создания Spring context.
 */
public final class PartitionRebuildCommandLine {

	/** Общий prefix параметров локальной rebuild-команды. */
	public static final String PROPERTY_PREFIX =
			"event-mosaic.maintenance.partition-rebuild";

	private static final String MODE_ARGUMENT = "--" + PROPERTY_PREFIX + ".mode";

	private PartitionRebuildCommandLine() {
	}

	/**
	 * Проверяет, что rebuild явно запрошен аргументом командной строки, а не
	 * скрытой настройкой окружения.
	 *
	 * @param arguments исходные аргументы запуска приложения
	 * @return {@code true}, если среди аргументов есть option режима rebuild
	 */
	public static boolean isRequested(String[] arguments) {
		Objects.requireNonNull(arguments, "arguments must not be null");
		return Arrays.stream(arguments)
				.anyMatch(argument -> MODE_ARGUMENT.equals(argument)
						|| argument.startsWith(MODE_ARGUMENT + "="));
	}
}
