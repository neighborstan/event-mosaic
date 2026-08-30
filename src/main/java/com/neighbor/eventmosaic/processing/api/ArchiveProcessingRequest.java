package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.IndexWriteMode;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Независимый от ingestion state запрос обработки одного подготовленного CSV.
 *
 * @param kind вид архива GDELT
 * @param sourceUpdateTime время исходного выпуска
 * @param sourceArchiveKey точный ключ версии архива
 * @param processingFingerprint fingerprint проекции и mapping revision
 * @param indexTargets exact generation, зафиксированная для обычного attempt
 * или теневого rebuild
 * @param writeMode обычная запись либо заполнение теневой rebuild generation
 * @param csvPath путь к подготовленному CSV
 * @param receiptPageSize максимальный размер PIT/search_after страницы receipt
 * @param operationBudget общая монотонная граница времени и проверка владения циклом
 */
public record ArchiveProcessingRequest(
		GdeltArchiveKind kind,
		Instant sourceUpdateTime,
		String sourceArchiveKey,
		String processingFingerprint,
		ActiveIndexTargets indexTargets,
		IndexWriteMode writeMode,
		Path csvPath,
		int receiptPageSize,
		OperationBudget operationBudget
) {

	/** Проверяет обязательный source context до открытия CSV. */
	public ArchiveProcessingRequest {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		requireText(sourceArchiveKey, "sourceArchiveKey");
		requireText(processingFingerprint, "processingFingerprint");
		Objects.requireNonNull(indexTargets, "indexTargets must not be null");
		Objects.requireNonNull(writeMode, "writeMode must not be null");
		Objects.requireNonNull(csvPath, "csvPath must not be null");
		Objects.requireNonNull(operationBudget, "operationBudget must not be null");
		if (receiptPageSize <= 0 || receiptPageSize > 10_000) {
			throw new IllegalArgumentException(
					"receiptPageSize must be between 1 and 10000");
		}
	}

	/** Создает обычный запрос с production budget owning cycle. */
	public ArchiveProcessingRequest(
			GdeltArchiveKind kind,
			Instant sourceUpdateTime,
			String sourceArchiveKey,
			String processingFingerprint,
			ActiveIndexTargets indexTargets,
			Path csvPath,
			int receiptPageSize,
			OperationBudget operationBudget
	) {
		this(
				kind,
				sourceUpdateTime,
				sourceArchiveKey,
				processingFingerprint,
				indexTargets,
				IndexWriteMode.ACTIVE,
				csvPath,
				receiptPageSize,
				operationBudget);
	}

	/** Создает запрос с переданным размером receipt и отдельным test budget. */
	public ArchiveProcessingRequest(
			GdeltArchiveKind kind,
			Instant sourceUpdateTime,
			String sourceArchiveKey,
			String processingFingerprint,
			ActiveIndexTargets indexTargets,
			Path csvPath,
			int receiptPageSize
	) {
		this(
				kind,
				sourceUpdateTime,
				sourceArchiveKey,
				processingFingerprint,
				indexTargets,
				IndexWriteMode.ACTIVE,
				csvPath,
				receiptPageSize,
				OperationBudget.start(Duration.ofDays(1)));
	}

	/** Создает запрос с production default страницы receipt. */
	public ArchiveProcessingRequest(
			GdeltArchiveKind kind,
			Instant sourceUpdateTime,
			String sourceArchiveKey,
			String processingFingerprint,
			ActiveIndexTargets indexTargets,
			Path csvPath
	) {
		this(
				kind,
				sourceUpdateTime,
				sourceArchiveKey,
				processingFingerprint,
				indexTargets,
				IndexWriteMode.ACTIVE,
				csvPath,
				500,
				OperationBudget.start(Duration.ofDays(1)));
	}

	private static void requireText(String value, String fieldName) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName + " must not be blank");
		}
	}
}
