package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Внешнее представление плотного снимка данных для карты стран. Оно отделяет
 * HTTP-контракт от поисковой модели и намеренно не повторяет геометрию, названия,
 * происхождение границ или признаки спорных территорий.
 *
 * @param snapshot точные границы и версии снимка
 * @param coverage доступная оценка полноты данных
 * @param quality общие счетчики размещения событий
 * @param regions все регионы каталога в стабильном порядке
 */
public record CountryMapSnapshotResponse(
		SnapshotResponse snapshot,
		CoverageResponse coverage,
		QualityResponse quality,
		List<RegionResponse> regions
) {

	/** Проверяет обязательные части ответа и защищает список регионов. */
	public CountryMapSnapshotResponse {
		Objects.requireNonNull(snapshot, "snapshot must not be null");
		Objects.requireNonNull(coverage, "coverage must not be null");
		Objects.requireNonNull(quality, "quality must not be null");
		regions = List.copyOf(Objects.requireNonNull(regions, "regions must not be null"));
	}

	/**
	 * Создает внешний DTO из проверенной поисковой модели.
	 *
	 * @param source внутренний снимок поискового модуля
	 * @return независимое HTTP-представление с той же канонической очередностью
	 */
	public static CountryMapSnapshotResponse from(CountryMapSnapshot source) {
		Objects.requireNonNull(source, "source must not be null");
		return new CountryMapSnapshotResponse(
				new SnapshotResponse(
						source.from(),
						source.to(),
						source.geometryVersion(),
						source.toneModelVersion()),
				CoverageResponse.from(source.coverage()),
				QualityResponse.from(source.quality()),
				source.regions().stream().map(RegionResponse::from).toList());
	}

	/**
	 * Точные серверные границы и версии данных.
	 *
	 * @param from включенная граница окна
	 * @param to исключенная граница окна
	 * @param geometryVersion версия отдельного статического набора геометрии
	 * @param toneModelVersion версия фактической группировки тональности
	 */
	public record SnapshotResponse(
			Instant from,
			Instant to,
			String geometryVersion,
			String toneModelVersion
	) {
	}

	/**
	 * Публичная оценка полноты Event-данных.
	 *
	 * @param status стабильное значение COMPLETE, PARTIAL или UNKNOWN
	 * @param missingIntervals пустой список для COMPLETE, непустой для PARTIAL
	 *                         или явный {@code null} для UNKNOWN
	 */
	public record CoverageResponse(
			String status,
			List<MissingIntervalResponse> missingIntervals
	) {

		/** Сохраняет различие между неизвестным и доказанно пустым списком. */
		public CoverageResponse {
			Objects.requireNonNull(status, "status must not be null");
			if (missingIntervals != null) {
				missingIntervals = List.copyOf(missingIntervals);
			}
		}

		private static CoverageResponse from(CountryMapSnapshot.Coverage source) {
			List<MissingIntervalResponse> intervals = source.missingIntervals() == null
					? null
					: source.missingIntervals().stream()
							.map(MissingIntervalResponse::from)
							.toList();
			return new CoverageResponse(source.status().name(), intervals);
		}
	}

	/**
	 * Один полуоткрытый интервал отсутствующих Event-данных.
	 *
	 * @param from включенная граница пропуска
	 * @param to исключенная граница пропуска
	 */
	public record MissingIntervalResponse(Instant from, Instant to) {

		private static MissingIntervalResponse from(
				CountryMapSnapshot.MissingInterval source
		) {
			return new MissingIntervalResponse(source.from(), source.to());
		}
	}

	/**
	 * Общие счетчики географического размещения и ограниченные причины.
	 *
	 * @param eligibleEventCount все Event выбранного окна
	 * @param mappedEventCount Event с точным регионом
	 * @param unlocatedEventCount Event без надежной Action location
	 * @param unmappedEventCount Event без точного соответствия каталогу
	 * @param unlocatedReasonCounts полный список причин отсутствующей location
	 * @param unmappedReasonCounts полный список причин отсутствующего mapping
	 */
	public record QualityResponse(
			long eligibleEventCount,
			long mappedEventCount,
			long unlocatedEventCount,
			long unmappedEventCount,
			List<ReasonCountResponse> unlocatedReasonCounts,
			List<ReasonCountResponse> unmappedReasonCounts
	) {

		/** Защищает оба списка причин от изменения после создания ответа. */
		public QualityResponse {
			unlocatedReasonCounts = List.copyOf(Objects.requireNonNull(
					unlocatedReasonCounts,
					"unlocatedReasonCounts must not be null"));
			unmappedReasonCounts = List.copyOf(Objects.requireNonNull(
					unmappedReasonCounts,
					"unmappedReasonCounts must not be null"));
		}

		private static QualityResponse from(CountryMapSnapshot.Quality source) {
			return new QualityResponse(
					source.eligibleEventCount(),
					source.mappedEventCount(),
					source.unlocatedEventCount(),
					source.unmappedEventCount(),
					source.unlocatedReasonCounts().stream()
							.map(reason -> new ReasonCountResponse(
									reason.reason().name(),
									reason.eventCount()))
							.toList(),
					source.unmappedReasonCounts().stream()
							.map(reason -> new ReasonCountResponse(
									reason.reason().name(),
									reason.eventCount()))
							.toList());
		}
	}

	/**
	 * Счетчик одной стабильной причины без внутреннего текста ошибки.
	 *
	 * @param reason публичное машинно-читаемое значение
	 * @param eventCount число событий этой категории
	 */
	public record ReasonCountResponse(String reason, long eventCount) {
	}

	/**
	 * Одна строка полного каталога стран.
	 *
	 * @param regionId стабильный идентификатор из geometry manifest
	 * @param eventCount все точно размещенные Event региона
	 * @param coloredEventCount Event с известной тональностью
	 * @param missingToneEventCount Event без тональности
	 * @param toneCounts фактические счетчики трех групп sign-v1
	 */
	public record RegionResponse(
			String regionId,
			long eventCount,
			long coloredEventCount,
			long missingToneEventCount,
			ToneCountsResponse toneCounts
	) {

		private static RegionResponse from(CountryMapSnapshot.Region source) {
			return new RegionResponse(
					source.regionId(),
					source.eventCount(),
					source.coloredEventCount(),
					source.missingToneEventCount(),
					ToneCountsResponse.from(source.toneCounts()));
		}
	}

	/**
	 * Фактические счетчики отрицательной, нулевой и положительной тональности.
	 *
	 * @param negative Event со значением меньше нуля
	 * @param zero Event с настоящим числовым нулем
	 * @param positive Event со значением больше нуля
	 */
	public record ToneCountsResponse(long negative, long zero, long positive) {

		private static ToneCountsResponse from(CountryMapSnapshot.ToneCounts source) {
			return new ToneCountsResponse(
					source.negative(),
					source.zero(),
					source.positive());
		}
	}
}
