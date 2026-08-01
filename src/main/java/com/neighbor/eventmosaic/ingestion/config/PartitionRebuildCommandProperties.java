package com.neighbor.eventmosaic.ingestion.config;

import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Параметры поддерживаемой локальной команды восстановления одной partition.
 * Наличие mode в аргументах запуска переводит приложение в однократный режим
 * без HTTP-сервера.
 */
@ConfigurationProperties(PartitionRebuildCommandLine.PROPERTY_PREFIX)
public record PartitionRebuildCommandProperties(
		Mode mode,
		String partitionKey,
		Path planFile,
		String actor,
		String reasonCode
) {

	/** Возвращает обязательный partition key inspect режима. */
	public String requirePartitionKey() {
		if (partitionKey == null || !partitionKey.matches("p[0-9]{8}")) {
			throw new IllegalArgumentException(
					"partition-key must be an exact P7D key");
		}
		return partitionKey;
	}

	/** Возвращает обязательный путь plan для create-only inspect или execute. */
	public Path requirePlanFile() {
		if (planFile == null) {
			throw new IllegalArgumentException("plan-file is required");
		}
		return planFile;
	}

	/** Возвращает обязательную bounded identity оператора. */
	public String requireActor() {
		if (actor == null || !actor.matches("[A-Za-z0-9._-]{1,64}")) {
			throw new IllegalArgumentException("actor has invalid format");
		}
		return actor;
	}

	/** Возвращает обязательный bounded reason code. */
	public String requireReasonCode() {
		if (reasonCode == null || !reasonCode.matches("[A-Z][A-Z0-9_]{0,63}")) {
			throw new IllegalArgumentException("reason-code has invalid format");
		}
		return reasonCode;
	}

	/** Два явно разделенных local maintenance режима. */
	public enum Mode {
		INSPECT_REBUILD,
		REBUILD_PARTITION
	}
}
