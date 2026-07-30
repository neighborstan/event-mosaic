package com.neighbor.eventmosaic.gdelt.api;

/**
 * Синхронно обрабатывает одну GDELT record с ее source provenance.
 *
 * <p>Выброшенное consumer исключение останавливает reader и пробрасывается без
 * wrapping. Повторное чтение может снова доставить уже обработанный prefix.</p>
 *
 * @param <T> тип raw Event или Mention
 */
@FunctionalInterface
public interface GdeltRecordConsumer<T> {

	/**
	 * Обрабатывает одну валидную запись до чтения следующей.
	 *
	 * @param csvRecord raw value и 1-based physical line number
	 */
	void accept(GdeltCsvRecord<T> csvRecord);
}
