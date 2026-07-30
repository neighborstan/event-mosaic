package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.EventDetailsQuery;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Публикует минимальный read-only endpoint деталей GDELT Event.
 */
@Validated
@RestController
@RequestMapping("/api/v1/events")
public class EventDetailsController {

	private final EventDetailsQuery eventDetailsQuery;

	/**
	 * Создает controller поверх публичной search boundary.
	 *
	 * @param eventDetailsQuery поиск read model
	 */
	public EventDetailsController(EventDetailsQuery eventDetailsQuery) {
		this.eventDetailsQuery = eventDetailsQuery;
	}

	/**
	 * Возвращает Event details или 404 для отсутствующего Event.
	 *
	 * @param eventId положительный GDELT GlobalEventId
	 * @return HTTP response с details либо без body для 404
	 */
	@GetMapping("/{eventId}")
	public ResponseEntity<EventDetailsResponse> findEvent(
			@PathVariable @Positive long eventId
	) {
		return ResponseEntity.of(
				eventDetailsQuery.findById(eventId).map(EventDetailsResponse::from));
	}
}
