package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@DisplayName("Однократный запуск загрузки")
class IngestionOneShotRunnerTest {

	private final IngestionCycleCoordinator coordinator = mock(IngestionCycleCoordinator.class);
	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withBean(IngestionCycleCoordinator.class, () -> coordinator)
			.withUserConfiguration(RunnerConfiguration.class);

	@Test
	@DisplayName("По умолчанию runner отсутствует и загрузка не запускается")
	void isAbsentAndDoesNotCallSourceByDefault() {
		contextRunner.run(context -> {
			assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
			verifyNoInteractions(coordinator);
		});
	}

	@Test
	@DisplayName("Явный флаг создает runner и запускает ровно один цикл")
	void explicitFlagCreatesRunnerThatExecutesExactlyOneCycle() {
		contextRunner
				.withPropertyValues("event-mosaic.ingestion.gdelt.one-shot-enabled=true")
				.run(context -> {
					IngestionOneShotRunner runner = context.getBean(IngestionOneShotRunner.class);

					runner.run(new DefaultApplicationArguments(new String[0]));

					verify(coordinator).runCycle();
				});
	}

	@Test
	@DisplayName("Runner заменяет неожиданную ошибку безопасной причиной остановки")
	void sanitizesUnexpectedFailureAtApplicationBoundary() {
		IllegalStateException unsafe = new IllegalStateException("secret runtime detail");
		when(coordinator.runCycle()).thenThrow(unsafe);
		IngestionOneShotRunner runner = new IngestionOneShotRunner(coordinator);
		DefaultApplicationArguments arguments = new DefaultApplicationArguments(new String[0]);

		assertThatThrownBy(() -> runner.run(arguments))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage(IngestionErrorCode.INTERNAL_ERROR.code()
						+ ": "
						+ IngestionErrorCode.INTERNAL_ERROR.safeMessage())
				.hasMessageNotContaining("secret runtime detail")
				.hasNoCause();
	}

	@Test
	@DisplayName("Runner сохраняет безопасный код cooperative interruption")
	void preservesCooperativeInterruptionCode() {
		when(coordinator.runCycle()).thenThrow(new IngestionInterruptedException());
		IngestionOneShotRunner runner = new IngestionOneShotRunner(coordinator);
		DefaultApplicationArguments arguments = new DefaultApplicationArguments(new String[0]);

		assertThatThrownBy(() -> runner.run(arguments))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage(IngestionErrorCode.OPERATION_INTERRUPTED.code()
						+ ": "
						+ IngestionErrorCode.OPERATION_INTERRUPTED.safeMessage())
				.hasNoCause();
	}

	@Test
	@DisplayName("Runner сохраняет отдельный безопасный исход потери global ownership")
	void preservesGlobalOwnershipLossOutcome() {
		when(coordinator.runCycle()).thenThrow(new OperationOwnershipLostException());
		IngestionOneShotRunner runner = new IngestionOneShotRunner(coordinator);
		DefaultApplicationArguments arguments = new DefaultApplicationArguments(new String[0]);

		assertThatThrownBy(() -> runner.run(arguments))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage(IngestionCycleOutcome.OWNERSHIP_LOST.name()
						+ ": Ingestion cycle ownership was lost")
				.hasNoCause();
	}

	@Test
	@DisplayName("Занятый global cycle остается различимым успешным one-shot outcome")
	void activeGlobalCycleIsHandledWithoutPipelineBypass() {
		when(coordinator.runCycle()).thenReturn(IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);
		IngestionOneShotRunner runner = new IngestionOneShotRunner(coordinator);

		runner.run(new DefaultApplicationArguments(new String[0]));

		verify(coordinator).runCycle();
	}

	@Configuration(proxyBeanMethods = false)
	@Import(IngestionOneShotRunner.class)
	static class RunnerConfiguration {
	}
}
