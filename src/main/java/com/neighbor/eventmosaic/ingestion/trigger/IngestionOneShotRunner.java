package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.IngestionRunService;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Однократно запускает acquisition cycle после старта приложения, когда это
 * явно разрешено свойством {@code one-shot-enabled}.
 */
@Component
@ConditionalOnProperty(
		prefix = "event-mosaic.ingestion.gdelt",
		name = "one-shot-enabled",
		havingValue = "true"
)
public class IngestionOneShotRunner implements ApplicationRunner {

	private final IngestionRunService ingestionRunService;

	/**
	 * Создает runner для основного orchestration service.
	 *
	 * @param ingestionRunService сервис одного ingestion cycle
	 */
	public IngestionOneShotRunner(IngestionRunService ingestionRunService) {
		this.ingestionRunService = ingestionRunService;
	}

	/**
	 * Выполняет один cycle; параметры командной строки на поведение не влияют.
	 *
	 * @param arguments параметры запуска Spring Boot
	 */
	@Override
	public void run(ApplicationArguments arguments) {
		try {
			ingestionRunService.runLatestUpdate();
		} catch (ApplicationException exception) {
			throw new IllegalStateException(
					exception.errorCode().code() + ": " + exception.errorCode().safeMessage());
		} catch (RuntimeException _) {
			throw new IllegalStateException(
					IngestionErrorCode.INTERNAL_ERROR.code()
							+ ": "
							+ IngestionErrorCode.INTERNAL_ERROR.safeMessage());
		}
	}
}
