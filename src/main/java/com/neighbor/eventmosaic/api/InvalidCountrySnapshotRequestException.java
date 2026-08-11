package com.neighbor.eventmosaic.api;

/**
 * Сообщает, что клиент попытался передать параметры в серверный снимок карты.
 * Такой запрос отклоняется целиком, чтобы неподдержанный параметр не выглядел
 * как успешно примененный фильтр.
 */
final class InvalidCountrySnapshotRequestException extends RuntimeException {

	private static final long serialVersionUID = 1L;
}
