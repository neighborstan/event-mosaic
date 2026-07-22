package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.IngestionRunService;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@DisplayName("Однократный запуск загрузки")
class IngestionOneShotRunnerTest {

	private final IngestionRunService ingestionRunService = mock(IngestionRunService.class);
	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withBean(IngestionRunService.class, () -> ingestionRunService)
			.withUserConfiguration(RunnerConfiguration.class);

	@Test
	@DisplayName("По умолчанию runner отсутствует и загрузка не запускается")
	void isAbsentAndDoesNotCallSourceByDefault() {
		contextRunner.run(context -> {
			assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
			verifyNoInteractions(ingestionRunService);
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

					verify(ingestionRunService).runLatestUpdate();
				});
	}

	@Test
	@DisplayName("Runner заменяет неожиданную ошибку безопасной причиной остановки")
	void sanitizesUnexpectedFailureAtApplicationBoundary() {
		IllegalStateException unsafe = new IllegalStateException("secret runtime detail");
		when(ingestionRunService.runLatestUpdate()).thenThrow(unsafe);
		IngestionOneShotRunner runner = new IngestionOneShotRunner(ingestionRunService);
		DefaultApplicationArguments arguments = new DefaultApplicationArguments(new String[0]);

		assertThatThrownBy(() -> runner.run(arguments))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage(IngestionErrorCode.INTERNAL_ERROR.code()
						+ ": "
						+ IngestionErrorCode.INTERNAL_ERROR.safeMessage())
				.hasMessageNotContaining("secret runtime detail")
				.hasNoCause();
	}

	@Configuration(proxyBeanMethods = false)
	@Import(IngestionOneShotRunner.class)
	static class RunnerConfiguration {
	}
}
