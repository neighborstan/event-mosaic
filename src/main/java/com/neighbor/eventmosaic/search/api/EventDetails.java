package com.neighbor.eventmosaic.search.api;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/**
 * Неизменяемая поисковая проекция одного события и связанных документов.
 *
 * @param event identity и даты события
 * @param actors участники события
 * @param classification классификация и оценки действия
 * @param location выбранная location или {@code null}
 * @param sources ограниченный список уникальных source documents
 */
public record EventDetails(
		Event event,
		Actors actors,
		Classification classification,
		Location location,
		List<SourceDocument> sources
) {

	/** Проверяет identity, обязательные даты и копирует список источников. */
	public EventDetails {
		Objects.requireNonNull(event, "event must not be null");
		Objects.requireNonNull(actors, "actors must not be null");
		Objects.requireNonNull(classification, "classification must not be null");
		sources = List.copyOf(sources);
	}

	/**
	 * Identity и provider dates события.
	 *
	 * @param eventId глобальный идентификатор GDELT Event
	 * @param eventDate календарный день события
	 * @param dateAdded время добавления события в GDELT
	 */
	public record Event(long eventId, LocalDate eventDate, Instant dateAdded) {

		/** Проверяет положительный ID и обязательные даты. */
		public Event {
			if (eventId <= 0) {
				throw new IllegalArgumentException("eventId must be positive");
			}
			Objects.requireNonNull(eventDate, "eventDate must not be null");
			Objects.requireNonNull(dateAdded, "dateAdded must not be null");
		}
	}

	/**
	 * Краткая проекция участника события.
	 *
	 * @param code CAMEO-код или {@code null}
	 * @param name отображаемое имя или {@code null}
	 */
	public record Actor(String code, String name) {
	}

	/**
	 * Пара участников Event.
	 *
	 * @param actor1 первый участник
	 * @param actor2 второй участник
	 */
	public record Actors(Actor actor1, Actor actor2) {
	}

	/**
	 * Минимальная CAMEO-классификация и числовые оценки.
	 *
	 * @param eventCode полный CAMEO-код действия
	 * @param eventRootCode корневой CAMEO-код действия
	 * @param quadClass CAMEO-квадрант
	 * @param goldsteinScale оценка по шкале Гольдштейна
	 * @param averageTone средний тон документов
	 */
	public record Classification(
			String eventCode,
			String eventRootCode,
			Integer quadClass,
			Double goldsteinScale,
			Double averageTone
	) {
	}

	/**
	 * Выбранная географическая точка события.
	 *
	 * @param role источник точки в raw Event
	 * @param latitude широта
	 * @param longitude долгота
	 * @param name отображаемое имя или {@code null}
	 * @param countryCode код страны или {@code null}
	 * @param featureId GDELT feature id или {@code null}
	 */
	public record Location(
			String role,
			double latitude,
			double longitude,
			String name,
			String countryCode,
			String featureId
	) {

		/** Проверяет обязательную роль и допустимые координаты. */
		public Location {
			Objects.requireNonNull(role, "role must not be null");
			if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90) {
				throw new IllegalArgumentException("latitude must be between -90 and 90");
			}
			if (!Double.isFinite(longitude) || longitude < -180 || longitude > 180) {
				throw new IllegalArgumentException("longitude must be between -180 and 180");
			}
		}
	}

	/**
	 * Одна уникальная карточка источника, выбранная из Mention observations.
	 *
	 * @param sourceName имя источника или {@code null}
	 * @param identifier точный provider identifier
	 * @param observedAt время observation в GDELT
	 * @param tone тон исходного документа или {@code null}
	 */
	public record SourceDocument(
			String sourceName,
			String identifier,
			Instant observedAt,
			Double tone
	) {

		/** Проверяет обязательные provider identifier и observation time. */
		public SourceDocument {
			Objects.requireNonNull(identifier, "identifier must not be null");
			Objects.requireNonNull(observedAt, "observedAt must not be null");
		}
	}
}
