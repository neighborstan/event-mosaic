package com.neighbor.eventmosaic.ingestion.source;

import java.util.Objects;

/**
 * Хранит проверенный хвостовой фрагмент полного каталога GDELT. Если чтение началось с середины строки, эта
 * неполная строка уже удалена.
 *
 * @param completeLines полные строки фрагмента в исходной кодировке UTF-8
 * @param rangeStart смещение первого полученного байта от начала файла
 * @param rangeEnd смещение последнего полученного байта от начала файла
 * @param totalBytes полный размер файла
 * @param generation версия файла в Google Cloud Storage
 * @param etag дополнительный идентификатор той же версии файла
 */
record GdeltMasterTailSnapshot(
		byte[] completeLines,
		long rangeStart,
		long rangeEnd,
		long totalBytes,
		String generation,
		String etag
) {

	GdeltMasterTailSnapshot {
		completeLines = Objects.requireNonNull(
				completeLines, "completeLines must not be null").clone();
		if (rangeStart < 0 || rangeEnd < rangeStart || totalBytes <= rangeEnd) {
			throw new IllegalArgumentException("range must fit the master object");
		}
		generation = requireText(generation, "generation");
		etag = requireText(etag, "etag");
	}

	@Override
	public byte[] completeLines() {
		return completeLines.clone();
	}

	boolean startsAtObjectBeginning() {
		return rangeStart == 0;
	}

	private static String requireText(String value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
		return value;
	}
}
