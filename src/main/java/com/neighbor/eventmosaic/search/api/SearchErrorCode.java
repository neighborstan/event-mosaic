package com.neighbor.eventmosaic.search.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Стабильный каталог безопасных ошибок поискового read model.
 */
public enum SearchErrorCode implements ApplicationErrorCode {

	SEARCH_UNAVAILABLE(
			"SEARCH_UNAVAILABLE",
			"Сервис поиска временно недоступен");

	private final String code;
	private final String safeMessage;

	SearchErrorCode(String code, String safeMessage) {
		this.code = code;
		this.safeMessage = safeMessage;
	}

	@Override
	public String code() {
		return code;
	}

	@Override
	public String safeMessage() {
		return safeMessage;
	}
}
