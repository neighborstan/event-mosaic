package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.SearchAccessException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Преобразует безопасные ошибки Event read API в стабильные HTTP responses.
 */
@RestControllerAdvice(assignableTypes = EventDetailsController.class)
public class EventApiExceptionHandler {

	private static final Logger LOGGER =
			LoggerFactory.getLogger(EventApiExceptionHandler.class);

	/**
	 * Возвращает безопасный 400 для нарушений method validation.
	 *
	 * @return problem details без внутренних имен constraint
	 */
	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<ProblemDetail> handleConstraintViolation() {
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
				HttpStatus.BAD_REQUEST,
				"Идентификатор события должен быть положительным");
		problem.setTitle("Invalid event identifier");
		problem.setProperty("code", "INVALID_EVENT_ID");
		return ResponseEntity.badRequest().body(problem);
	}

	/**
	 * Возвращает 503 без исходного Elasticsearch reason.
	 *
	 * @param exception каталогизированная временная ошибка search
	 * @return problem details со стабильным code
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
				.addKeyValue("event", "event.details.search_unavailable")
				.addKeyValue("error_code", errorCode.code())
				.addKeyValue("http_status", HttpStatus.SERVICE_UNAVAILABLE.value())
				.log("Event details search is unavailable");
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(
				HttpStatus.SERVICE_UNAVAILABLE,
				errorCode.safeMessage());
		problem.setTitle("Search unavailable");
		problem.setProperty("code", errorCode.code());
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(problem);
	}
}
