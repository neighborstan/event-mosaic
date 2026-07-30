package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.EventDetails;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * HTTP-представление минимальных деталей Event и уникальных sources.
 *
 * @param event identity и даты события
 * @param actors участники события
 * @param classification классификация и оценки действия
 * @param location выбранная location или {@code null}
 * @param sources ограниченный список уникальных source documents
 */
public record EventDetailsResponse(
		EventResponse event,
		ActorsResponse actors,
		ClassificationResponse classification,
		LocationResponse location,
		List<SourceResponse> sources
) {

	/** Проверяет обязательные sections и защищает список sources от изменений. */
	public EventDetailsResponse {
		Objects.requireNonNull(event, "event must not be null");
		Objects.requireNonNull(actors, "actors must not be null");
		Objects.requireNonNull(classification, "classification must not be null");
		sources = List.copyOf(sources);
	}

	/**
	 * Создает внешний DTO из search projection.
	 *
	 * @param details внутренняя поисковая проекция
	 * @return независимый HTTP response
	 */
	public static EventDetailsResponse from(EventDetails details) {
		Objects.requireNonNull(details, "details must not be null");
		return new EventDetailsResponse(
				EventResponse.from(details.event()),
				ActorsResponse.from(details.actors()),
				ClassificationResponse.from(details.classification()),
				LocationResponse.from(details.location()),
				details.sources().stream().map(SourceResponse::from).toList()
		);
	}

	/**
	 * HTTP-представление identity и provider dates.
	 *
	 * @param eventId глобальный идентификатор события
	 * @param eventDate календарный день события
	 * @param dateAdded время добавления в GDELT
	 */
	public record EventResponse(long eventId, LocalDate eventDate, Instant dateAdded) {

		private static EventResponse from(EventDetails.Event event) {
			return new EventResponse(event.eventId(), event.eventDate(), event.dateAdded());
		}
	}

	/**
	 * HTTP-представление участника.
	 *
	 * @param code CAMEO-код или {@code null}
	 * @param name имя или {@code null}
	 */
	public record ActorResponse(String code, String name) {

		private static ActorResponse from(EventDetails.Actor actor) {
			return actor == null ? null : new ActorResponse(actor.code(), actor.name());
		}
	}

	/**
	 * HTTP-представление пары участников.
	 *
	 * @param actor1 первый участник
	 * @param actor2 второй участник
	 */
	public record ActorsResponse(ActorResponse actor1, ActorResponse actor2) {

		private static ActorsResponse from(EventDetails.Actors actors) {
			return new ActorsResponse(
					ActorResponse.from(actors.actor1()),
					ActorResponse.from(actors.actor2()));
		}
	}

	/**
	 * HTTP-представление CAMEO-классификации и оценок.
	 *
	 * @param eventCode полный CAMEO-код действия
	 * @param eventRootCode корневой CAMEO-код действия
	 * @param quadClass CAMEO-квадрант
	 * @param goldsteinScale оценка по шкале Гольдштейна
	 * @param averageTone средний тон документов
	 */
	public record ClassificationResponse(
			String eventCode,
			String eventRootCode,
			Integer quadClass,
			Double goldsteinScale,
			Double averageTone
	) {

		private static ClassificationResponse from(
				EventDetails.Classification classification
		) {
			return new ClassificationResponse(
					classification.eventCode(),
					classification.eventRootCode(),
					classification.quadClass(),
					classification.goldsteinScale(),
					classification.averageTone());
		}
	}

	/**
	 * HTTP-представление выбранной точки.
	 *
	 * @param role источник точки в raw Event
	 * @param latitude широта
	 * @param longitude долгота
	 * @param name имя или {@code null}
	 * @param countryCode код страны или {@code null}
	 * @param featureId feature id или {@code null}
	 */
	public record LocationResponse(
			String role,
			double latitude,
			double longitude,
			String name,
			String countryCode,
			String featureId
	) {

		private static LocationResponse from(EventDetails.Location location) {
			return location == null
					? null
					: new LocationResponse(
							location.role(),
							location.latitude(),
							location.longitude(),
							location.name(),
							location.countryCode(),
							location.featureId());
		}
	}

	/**
	 * HTTP-представление одного уникального source document.
	 *
	 * @param sourceName имя источника или {@code null}
	 * @param identifier точный provider identifier
	 * @param observedAt observation time
	 * @param tone тон документа или {@code null}
	 */
	public record SourceResponse(
			String sourceName,
			String identifier,
			Instant observedAt,
			Double tone
	) {

		private static SourceResponse from(EventDetails.SourceDocument source) {
			return new SourceResponse(
					source.sourceName(),
					source.identifier(),
					source.observedAt(),
					source.tone());
		}
	}
}
