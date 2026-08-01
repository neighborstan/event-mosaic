package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

/**
 * Независимый от ingestion state запрос обработки одного подготовленного CSV.
 *
 * @param kind вид архива GDELT
 * @param sourceUpdateTime время исходного выпуска
 * @param sourceArchiveKey точный ключ версии архива
 * @param processingFingerprint fingerprint проекции и mapping revision
 * @param indexTargets exact ACTIVE generation, зафиксированная для attempt
 * @param csvPath путь к подготовленному CSV
 */
public record ArchiveProcessingRequest(
		GdeltArchiveKind kind,
		Instant sourceUpdateTime,
		String sourceArchiveKey,
		String processingFingerprint,
		ActiveIndexTargets indexTargets,
		Path csvPath
) {

	/** Проверяет обязательный source context до открытия CSV. */
	public ArchiveProcessingRequest {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		requireText(sourceArchiveKey, "sourceArchiveKey");
		requireText(processingFingerprint, "processingFingerprint");
		Objects.requireNonNull(indexTargets, "indexTargets must not be null");
		Objects.requireNonNull(csvPath, "csvPath must not be null");
	}

	private static void requireText(String value, String fieldName) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName + " must not be blank");
		}
	}
}
