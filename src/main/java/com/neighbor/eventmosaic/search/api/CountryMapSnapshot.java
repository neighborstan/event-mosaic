package com.neighbor.eventmosaic.search.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Неизменяемый серверный снимок событий для полного каталога стран. Модель
 * хранит фактические счетчики, версию геометрии и отдельную оценку полноты,
 * чтобы потребитель мог отличить нулевые данные от пропусков загрузки.
 *
 * @param from включенная граница суточного окна
 * @param to исключенная граница суточного окна
 * @param geometryVersion версия опубликованной геометрии и каталога регионов
 * @param toneModelVersion версия правил группировки тональности
 * @param coverage доказанная полнота Event-слотов окна
 * @param quality общие счетчики размещения событий
 * @param regions все регионы каталога в каноническом порядке
 */
public record CountryMapSnapshot(
		Instant from,
		Instant to,
		String geometryVersion,
		String toneModelVersion,
		Coverage coverage,
		Quality quality,
		List<Region> regions
) {

	/** Версия фактической модели, которая различает семь диапазонов tone. */
	public static final String TONE_MODEL_VERSION = "tone-bands-v1";

	private static final Duration SNAPSHOT_DURATION = Duration.ofHours(24);
	private static final long SOURCE_CADENCE_SECONDS = Duration.ofMinutes(15).toSeconds();
	private static final Pattern GEOMETRY_VERSION_PATTERN =
			Pattern.compile("country-v[1-9][0-9]*");

	/** Проверяет форму окна, версии, порядок регионов и общие равенства счетчиков. */
	public CountryMapSnapshot {
		Objects.requireNonNull(from, "from must not be null");
		Objects.requireNonNull(to, "to must not be null");
		if (!SNAPSHOT_DURATION.equals(Duration.between(from, to))) {
			throw new IllegalArgumentException("snapshot window must span exactly 24 hours");
		}
		if (!isCadenceBoundary(from) || !isCadenceBoundary(to)) {
			throw new IllegalArgumentException(
					"snapshot window boundaries must use the 15-minute UTC grid");
		}
		if (geometryVersion == null
				|| !GEOMETRY_VERSION_PATTERN.matcher(geometryVersion).matches()) {
			throw new IllegalArgumentException("geometryVersion must identify country-vN");
		}
		if (!TONE_MODEL_VERSION.equals(toneModelVersion)) {
			throw new IllegalArgumentException("toneModelVersion must be tone-bands-v1");
		}
		Objects.requireNonNull(coverage, "coverage must not be null");
		Objects.requireNonNull(quality, "quality must not be null");
		regions = List.copyOf(Objects.requireNonNull(regions, "regions must not be null"));
		if (regions.isEmpty()) {
			throw new IllegalArgumentException("regions must not be empty");
		}

		String previousRegionId = null;
		long mappedFromRegions = 0;
		for (Region region : regions) {
			Objects.requireNonNull(region, "region must not be null");
			if (previousRegionId != null
					&& previousRegionId.compareTo(region.regionId()) >= 0) {
				throw new IllegalArgumentException(
						"regions must use canonical ascending order");
			}
			previousRegionId = region.regionId();
			mappedFromRegions = addCounts(
					"mapped region count",
					mappedFromRegions,
					region.eventCount());
		}
		if (mappedFromRegions != quality.mappedEventCount()) {
			throw new IllegalArgumentException(
					"mappedEventCount must equal the sum of region event counts");
		}
		if (coverage.missingIntervals() != null) {
			for (MissingInterval interval : coverage.missingIntervals()) {
				if (interval.from().isBefore(from) || interval.to().isAfter(to)) {
					throw new IllegalArgumentException(
							"coverage interval must stay inside the snapshot window");
				}
			}
		}
	}

	/** Состояния доказуемой полноты суточного набора Event. */
	public enum CoverageStatus {

		/** Все ожидаемые Event-слоты подтверждены. */
		COMPLETE,

		/** Evidence доступен, но часть Event-слотов отсутствует. */
		PARTIAL,

		/** Полноту нельзя надежно определить по диагностическому контуру. */
		UNKNOWN
	}

	/**
	 * Оценка полноты и объединенные интервалы отсутствующих Event-слотов.
	 *
	 * @param status результат проверки полноты
	 * @param missingIntervals пустой список для COMPLETE, непустой для PARTIAL
	 *                         или {@code null} для UNKNOWN
	 */
	public record Coverage(
			CoverageStatus status,
			List<MissingInterval> missingIntervals
	) {

		/** Проверяет согласованность статуса и формы списка пропусков. */
		public Coverage {
			Objects.requireNonNull(status, "status must not be null");
			if (status == CoverageStatus.UNKNOWN) {
				if (missingIntervals != null) {
					throw new IllegalArgumentException(
							"UNKNOWN coverage must not expose missing intervals");
				}
			}
			else {
				missingIntervals = List.copyOf(Objects.requireNonNull(
						missingIntervals,
						"missingIntervals must not be null"));
				if (status == CoverageStatus.COMPLETE && !missingIntervals.isEmpty()) {
					throw new IllegalArgumentException(
							"COMPLETE coverage must not contain missing intervals");
				}
				if (status == CoverageStatus.PARTIAL && missingIntervals.isEmpty()) {
					throw new IllegalArgumentException(
							"PARTIAL coverage must contain missing intervals");
				}
				for (int index = 1; index < missingIntervals.size(); index++) {
					MissingInterval previous = missingIntervals.get(index - 1);
					MissingInterval current = missingIntervals.get(index);
					if (!previous.to().isBefore(current.from())) {
						throw new IllegalArgumentException(
								"missing intervals must be ordered, disjoint and non-adjacent");
					}
				}
			}
		}
	}

	/**
	 * Полуоткрытый интервал отсутствующих Event-данных.
	 *
	 * @param from включенная граница пропуска
	 * @param to исключенная граница пропуска
	 */
	public record MissingInterval(Instant from, Instant to) {

		/** Проверяет наличие и правильный порядок границ. */
		public MissingInterval {
			Objects.requireNonNull(from, "from must not be null");
			Objects.requireNonNull(to, "to must not be null");
			if (!from.isBefore(to)) {
				throw new IllegalArgumentException("missing interval from must be before to");
			}
			if (!isCadenceBoundary(from) || !isCadenceBoundary(to)) {
				throw new IllegalArgumentException(
						"missing interval boundaries must use the 15-minute UTC grid");
			}
		}
	}

	/**
	 * Общие счетчики размещения событий и причины, по которым страна не выбрана.
	 *
	 * @param eligibleEventCount все Event выбранного окна
	 * @param mappedEventCount Event, точно связанных с регионом
	 * @param unlocatedEventCount Event без подходящей Action location
	 * @param unmappedEventCount Event с Action location, но без точного региона
	 * @param unlocatedReasonCounts полный список причин отсутствующей location
	 * @param unmappedReasonCounts полный список причин отсутствующего mapping
	 */
	public record Quality(
			long eligibleEventCount,
			long mappedEventCount,
			long unlocatedEventCount,
			long unmappedEventCount,
			List<UnlocatedReasonCount> unlocatedReasonCounts,
			List<UnmappedReasonCount> unmappedReasonCounts
	) {

		/** Проверяет неотрицательность, полный порядок причин и итоговые равенства. */
		public Quality {
			requireNonNegative(eligibleEventCount, "eligibleEventCount");
			requireNonNegative(mappedEventCount, "mappedEventCount");
			requireNonNegative(unlocatedEventCount, "unlocatedEventCount");
			requireNonNegative(unmappedEventCount, "unmappedEventCount");
			unlocatedReasonCounts = List.copyOf(Objects.requireNonNull(
					unlocatedReasonCounts,
					"unlocatedReasonCounts must not be null"));
			unmappedReasonCounts = List.copyOf(Objects.requireNonNull(
					unmappedReasonCounts,
					"unmappedReasonCounts must not be null"));

			requireUnlocatedReasonOrder(unlocatedReasonCounts);
			requireUnmappedReasonOrder(unmappedReasonCounts);
			long classified = addCounts(
					"eligible event count",
					mappedEventCount,
					unlocatedEventCount,
					unmappedEventCount);
			if (eligibleEventCount != classified) {
				throw new IllegalArgumentException(
						"eligibleEventCount must equal mapped, unlocated and unmapped counts");
			}
			long unlocatedFromReasons = 0;
			for (UnlocatedReasonCount reasonCount : unlocatedReasonCounts) {
				unlocatedFromReasons = addCounts(
						"unlocated reason count",
						unlocatedFromReasons,
						reasonCount.eventCount());
			}
			if (unlocatedEventCount != unlocatedFromReasons) {
				throw new IllegalArgumentException(
						"unlocatedEventCount must equal its reason counts");
			}
			long unmappedFromReasons = 0;
			for (UnmappedReasonCount reasonCount : unmappedReasonCounts) {
				unmappedFromReasons = addCounts(
						"unmapped reason count",
						unmappedFromReasons,
						reasonCount.eventCount());
			}
			if (unmappedEventCount != unmappedFromReasons) {
				throw new IllegalArgumentException(
						"unmappedEventCount must equal its reason counts");
			}
		}
	}

	/** Причины, по которым Event не имеет выбранной Action location. */
	public enum UnlocatedReason {

		/** ActionGeo отсутствовал или был непригоден для индексирования. */
		ACTION_GEO_MISSING_OR_INVALID,

		/** Индексатор выбрал географию одного из участников вместо ActionGeo. */
		ACTOR_FALLBACK,

		/** Зарезервированная ограниченная категория для иной известной роли. */
		OTHER
	}

	/**
	 * Счетчик одной причины отсутствующей Action location.
	 *
	 * @param reason ограниченная публичная причина
	 * @param eventCount число Event этой категории
	 */
	public record UnlocatedReasonCount(UnlocatedReason reason, long eventCount) {

		/** Проверяет обязательную причину и неотрицательный счетчик. */
		public UnlocatedReasonCount {
			Objects.requireNonNull(reason, "reason must not be null");
			requireNonNegative(eventCount, "eventCount");
		}
	}

	/** Причины, по которым Action country code не связан с регионом. */
	public enum UnmappedReason {

		/** Country code отсутствует в индексированном Action location. */
		COUNTRY_CODE_MISSING,

		/** Code не известен выбранной версии каталога. */
		UNKNOWN_COUNTRY_CODE,

		/** Для code нет отдельной территориальной геометрии. */
		NO_REGION_GEOMETRY,

		/** Code нельзя однозначно связать с одним регионом. */
		AMBIGUOUS_REGION_MAPPING,

		/** Catalog явно не поддерживает этот country-like code. */
		UNSUPPORTED_COUNTRY_CODE,

		/** Зарезервированная ограниченная категория известного остатка. */
		OTHER
	}

	/**
	 * Счетчик одной причины отсутствующего точного mapping.
	 *
	 * @param reason ограниченная публичная причина
	 * @param eventCount число Event этой категории
	 */
	public record UnmappedReasonCount(UnmappedReason reason, long eventCount) {

		/** Проверяет обязательную причину и неотрицательный счетчик. */
		public UnmappedReasonCount {
			Objects.requireNonNull(reason, "reason must not be null");
			requireNonNegative(eventCount, "eventCount");
		}
	}

	/** Семь фактических групп подтвержденной модели tone. */
	public enum Tone {

		/** Значение averageTone меньше или равно -8. */
		NEGATIVE_EXTREME,

		/** Значение averageTone больше -8, но меньше или равно -3. */
		NEGATIVE_STRONG,

		/** Значение averageTone больше -3, но меньше нуля. */
		NEGATIVE_MILD,

		/** Значение averageTone точно равно нулю. */
		ZERO,

		/** Значение averageTone больше нуля, но меньше 3. */
		POSITIVE_MILD,

		/** Значение averageTone не меньше 3, но меньше 8. */
		POSITIVE_STRONG,

		/** Значение averageTone не меньше 8. */
		POSITIVE_EXTREME
	}

	/**
	 * Счетчики семи групп фактического tone.
	 *
	 * @param negativeExtreme Event со значением не больше -8
	 * @param negativeStrong Event со значением от -8 исключительно до -3 включительно
	 * @param negativeMild Event со значением от -3 исключительно до нуля исключительно
	 * @param zero Event с averageTone, равным нулю
	 * @param positiveMild Event со значением от нуля исключительно до 3 исключительно
	 * @param positiveStrong Event со значением от 3 включительно до 8 исключительно
	 * @param positiveExtreme Event со значением не меньше 8
	 */
	public record ToneCounts(
			long negativeExtreme,
			long negativeStrong,
			long negativeMild,
			long zero,
			long positiveMild,
			long positiveStrong,
			long positiveExtreme
	) {

		/** Проверяет, что каждый счетчик неотрицателен. */
		public ToneCounts {
			requireNonNegative(negativeExtreme, "negativeExtreme");
			requireNonNegative(negativeStrong, "negativeStrong");
			requireNonNegative(negativeMild, "negativeMild");
			requireNonNegative(zero, "zero");
			requireNonNegative(positiveMild, "positiveMild");
			requireNonNegative(positiveStrong, "positiveStrong");
			requireNonNegative(positiveExtreme, "positiveExtreme");
			coloredEventCount();
		}

		/**
		 * Возвращает число событий выбранной группы tone.
		 *
		 * @param tone одна из семи групп tone-bands-v1
		 * @return число Event в таком диапазоне tone
		 */
		public long count(Tone tone) {
			return switch (Objects.requireNonNull(tone, "tone must not be null")) {
				case NEGATIVE_EXTREME -> negativeExtreme;
				case NEGATIVE_STRONG -> negativeStrong;
				case NEGATIVE_MILD -> negativeMild;
				case ZERO -> zero;
				case POSITIVE_MILD -> positiveMild;
				case POSITIVE_STRONG -> positiveStrong;
				case POSITIVE_EXTREME -> positiveExtreme;
			};
		}

		private long coloredEventCount() {
			return addCounts(
					"colored event count",
					negativeExtreme,
					negativeStrong,
					negativeMild,
					zero,
					positiveMild,
					positiveStrong,
					positiveExtreme);
		}
	}

	/**
	 * Одна строка плотного списка регионов.
	 *
	 * @param regionId стабильный идентификатор региона из geometry manifest
	 * @param eventCount все точно размещенные Event региона
	 * @param coloredEventCount Event с известным tone
	 * @param missingToneEventCount Event без tone
	 * @param toneCounts фактические счетчики семи групп tone-bands-v1
	 */
	public record Region(
			String regionId,
			long eventCount,
			long coloredEventCount,
			long missingToneEventCount,
			ToneCounts toneCounts
	) {

		/** Проверяет идентификатор и равенства региональных счетчиков. */
		public Region {
			if (regionId == null || regionId.isBlank()) {
				throw new IllegalArgumentException("regionId must not be blank");
			}
			requireNonNegative(eventCount, "eventCount");
			requireNonNegative(coloredEventCount, "coloredEventCount");
			requireNonNegative(missingToneEventCount, "missingToneEventCount");
			Objects.requireNonNull(toneCounts, "toneCounts must not be null");
			if (coloredEventCount != toneCounts.coloredEventCount()) {
				throw new IllegalArgumentException(
						"coloredEventCount must equal all seven tone band counts");
			}
			if (eventCount != addCounts(
					"region event count",
					coloredEventCount,
					missingToneEventCount)) {
				throw new IllegalArgumentException(
						"eventCount must equal colored and missing tone counts");
			}
		}
	}

	private static boolean isCadenceBoundary(Instant instant) {
		return instant.getNano() == 0
				&& Math.floorMod(instant.getEpochSecond(), SOURCE_CADENCE_SECONDS) == 0;
	}

	private static void requireUnlocatedReasonOrder(
			List<UnlocatedReasonCount> reasonCounts
	) {
		UnlocatedReason[] expected = UnlocatedReason.values();
		if (reasonCounts.size() != expected.length) {
			throw new IllegalArgumentException(
					"unlocatedReasonCounts must contain every reason exactly once");
		}
		for (int index = 0; index < expected.length; index++) {
			if (reasonCounts.get(index).reason() != expected[index]) {
				throw new IllegalArgumentException(
						"unlocatedReasonCounts must use stable enum order");
			}
		}
	}

	private static void requireUnmappedReasonOrder(
			List<UnmappedReasonCount> reasonCounts
	) {
		UnmappedReason[] expected = UnmappedReason.values();
		if (reasonCounts.size() != expected.length) {
			throw new IllegalArgumentException(
					"unmappedReasonCounts must contain every reason exactly once");
		}
		for (int index = 0; index < expected.length; index++) {
			if (reasonCounts.get(index).reason() != expected[index]) {
				throw new IllegalArgumentException(
						"unmappedReasonCounts must use stable enum order");
			}
		}
	}

	private static void requireNonNegative(long count, String label) {
		if (count < 0) {
			throw new IllegalArgumentException(label + " must not be negative");
		}
	}

	private static long addCounts(String label, long... counts) {
		long total = 0;
		try {
			for (long count : counts) {
				total = Math.addExact(total, count);
			}
			return total;
		}
		catch (ArithmeticException exception) {
			throw new IllegalArgumentException(label + " must fit in a 64-bit count", exception);
		}
	}
}
