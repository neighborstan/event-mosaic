package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Однократно запускает сквозной ingestion pipeline после старта приложения,
 * когда это явно разрешено свойством {@code one-shot-enabled}.
 */
@Component
@Conditional(IngestionRuntimeModeCondition.OneShot.class)
public class IngestionOneShotRunner implements ApplicationRunner {

	private final IngestionCycleCoordinator coordinator;

	/**
	 * Создает runner для основного orchestration service.
	 *
	 * @param coordinator единая граница global ownership и ingestion cycle
	 */
	public IngestionOneShotRunner(IngestionCycleCoordinator coordinator) {
		this.coordinator = coordinator;
	}

	/**
	 * Выполняет один cycle; параметры командной строки на поведение не влияют.
	 *
	 * @param arguments параметры запуска Spring Boot
	 */
	@Override
	public void run(ApplicationArguments arguments) {
		try {
			coordinator.runCycle();
		} catch (ApplicationException exception) {
			throw new IllegalStateException(
					exception.errorCode().code() + ": " + exception.errorCode().safeMessage());
		} catch (OperationOwnershipLostException _) {
			throw new IllegalStateException(
					IngestionCycleOutcome.OWNERSHIP_LOST.name()
							+ ": Ingestion cycle ownership was lost");
		} catch (RuntimeException _) {
			throw new IllegalStateException(
					IngestionErrorCode.INTERNAL_ERROR.code()
							+ ": "
							+ IngestionErrorCode.INTERNAL_ERROR.safeMessage());
		}
	}
}
