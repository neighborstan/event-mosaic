package com.neighbor.eventmosaic.search.api;

import java.util.Optional;

/**
 * Публичная read-only граница поиска деталей одного GDELT Event.
 */
public interface EventDetailsQuery {

	/**
	 * Ищет Event и ограниченные уникальные source documents.
	 *
	 * @param eventId положительный глобальный идентификатор события
	 * @return details либо empty, если событие или read-model index отсутствуют
	 * @throws SearchAccessException при временной недоступности Elasticsearch
	 */
	Optional<EventDetails> findById(long eventId);
}
