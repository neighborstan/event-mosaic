package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.SearchAccessException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Преобразует ожидаемые ошибки снимка карты в безопасные Problem Details.
 * Обработчик действует только для country endpoint и не меняет ответы других API.
 */
@RestControllerAdvice(assignableTypes = CountryMapSnapshotController.class)
public class CountryMapSnapshotApiExceptionHandler {

	private static final Logger LOGGER =
			LoggerFactory.getLogger(CountryMapSnapshotApiExceptionHandler.class);
	private static final String INVALID_REQUEST_CODE =
			"INVALID_COUNTRY_SNAPSHOT_REQUEST";

	/**
	 * Возвращает 400, когда клиент передал неподдерживаемую query string.
	 *
	 * @return Problem Details со стабильным кодом без содержимого запроса
	 */
	@ExceptionHandler(InvalidCountrySnapshotRequestException.class)
	public ResponseEntity<ProblemDetail> handleInvalidRequest() {
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
				HttpStatus.BAD_REQUEST,
				"Снимок карты не принимает параметры запроса");
		problem.setTitle("Некорректный запрос снимка карты");
		problem.setProperty("code", INVALID_REQUEST_CODE);
		return ResponseEntity.badRequest().body(problem);
	}

	/**
	 * Возвращает 503 без исходной причины отказа Elasticsearch.
	 *
	 * @param exception каталогизированная временная ошибка поиска
	 * @return Problem Details со стабильным безопасным кодом
	 */
	@ExceptionHandler(SearchAccessException.class)
	public ResponseEntity<ProblemDetail> handleSearchUnavailable(
			SearchAccessException exception
	) {
		var errorCode = exception.errorCode();
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						exception,
						errorCode.safeMessage()))
				.addKeyValue("event", "country.map.snapshot.search_unavailable")
				.addKeyValue("error_code", errorCode.code())
				.addKeyValue("http_status", HttpStatus.SERVICE_UNAVAILABLE.value())
				.log("Country map snapshot search is unavailable");
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
				HttpStatus.SERVICE_UNAVAILABLE,
				errorCode.safeMessage());
		problem.setTitle("Сервис поиска недоступен");
		problem.setProperty("code", errorCode.code());
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(problem);
	}
}
