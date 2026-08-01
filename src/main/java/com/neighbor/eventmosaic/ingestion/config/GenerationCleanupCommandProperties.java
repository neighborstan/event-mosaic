package com.neighbor.eventmosaic.ingestion.config;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import java.nio.file.Path;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Параметры серверной one-shot команды очистки одной технической generation.
 * Inspect выбирает exact UUID и создает новый plan-файл, execute читает тот же
 * файл и добавляет ограниченные audit-значения.
 */
@ConfigurationProperties(GenerationCleanupCommandLine.PROPERTY_PREFIX)
public record GenerationCleanupCommandProperties(
		Mode mode,
		String partitionKey,
		UUID generationUuid,
		Path planFile,
		String actor,
		String reasonCode
) {

	/** Возвращает обязательный exact partition key inspect режима. */
	public String requirePartitionKey() {
		if (partitionKey == null || !partitionKey.matches("p[0-9]{8}")) {
			throw new IllegalArgumentException(
					"partition-key must be an exact P7D key");
		}
		return partitionKey;
	}

	/** Возвращает обязательную durable identity очищаемой generation. */
	public UUID requireGenerationUuid() {
		if (generationUuid == null) {
			throw new IllegalArgumentException("generation-uuid is required");
		}
		return generationUuid;
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

	/** Два явно разделенных режима ручной очистки. */
	public enum Mode {
		INSPECT_CLEANUP,
		CLEANUP_GENERATION
	}
}
