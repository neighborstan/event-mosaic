package com.neighbor.eventmosaic.ingestion.staging;

import java.util.Objects;

/**
 * Возвращает ограниченный результат проверки staging storage.
 *
 * @param status доступность staging
 * @param errorCode стабильная причина результата
 * @param usableBytes доступное filesystem space или {@code -1}, если его нельзя
 * определить
 */
public record StagingStorageProbeResult(
		StagingStorageProbeStatus status,
		StagingStorageProbeErrorCode errorCode,
		long usableBytes
) {

	/** Проверяет согласованность статуса, причины и значения свободного места. */
	public StagingStorageProbeResult {
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(errorCode, "errorCode must not be null");
		if (status == StagingStorageProbeStatus.WRITABLE) {
			if (errorCode != StagingStorageProbeErrorCode.NONE || usableBytes < 0) {
				throw new IllegalArgumentException(
						"Writable staging result requires known usable bytes and no error");
			}
		} else if (errorCode == StagingStorageProbeErrorCode.NONE || usableBytes != -1) {
			throw new IllegalArgumentException(
					"Unavailable staging result requires an error and unknown usable bytes");
		}
	}

	/**
	 * Создает успешный результат с фактическим доступным местом.
	 *
	 * @param usableBytes доступное filesystem space
	 * @return успешный результат проверки
	 */
	public static StagingStorageProbeResult writable(long usableBytes) {
		return new StagingStorageProbeResult(
				StagingStorageProbeStatus.WRITABLE,
				StagingStorageProbeErrorCode.NONE,
				usableBytes);
	}

	/**
	 * Создает результат для небезопасного staging path.
	 *
	 * @param errorCode стабильная причина отказа
	 * @return результат проверки небезопасного path
	 */
	public static StagingStorageProbeResult unsafe(StagingStorageProbeErrorCode errorCode) {
		return new StagingStorageProbeResult(
				StagingStorageProbeStatus.UNSAFE,
				errorCode,
				-1);
	}

	/**
	 * Создает результат для недоступного staging storage.
	 *
	 * @param errorCode стабильная причина отказа
	 * @return результат неуспешной проверки
	 */
	public static StagingStorageProbeResult unavailable(StagingStorageProbeErrorCode errorCode) {
		return new StagingStorageProbeResult(
				StagingStorageProbeStatus.UNAVAILABLE,
				errorCode,
				-1);
	}

	/** Возвращает {@code true}, если staging прошел проверку записи и удаления. */
	public boolean isWritable() {
		return status == StagingStorageProbeStatus.WRITABLE;
	}
}
