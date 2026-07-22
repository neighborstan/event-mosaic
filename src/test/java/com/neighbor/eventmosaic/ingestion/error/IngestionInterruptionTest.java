package com.neighbor.eventmosaic.ingestion.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Кооперативное прерывание операции загрузки")
class IngestionInterruptionTest {

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	@DisplayName("I/O cause без interrupt flag не меняет смысл operation failure")
	void ioCauseWithoutInterruptFlagDoesNotBecomeInterruption() {
		assertThatCode(() -> IngestionInterruption.throwIfRequested(new IOException("failure")))
				.doesNotThrowAnyException();
		assertThat(Thread.currentThread().isInterrupted()).isFalse();
	}

	@Test
	@DisplayName("Установленный interrupt flag завершает operation и сохраняет cause")
	void interruptFlagStopsOperationAndPreservesCause() {
		IOException cause = new IOException("interrupted operation");
		Thread.currentThread().interrupt();

		assertThatExceptionOfType(IngestionInterruptedException.class)
				.isThrownBy(() -> IngestionInterruption.throwIfRequested(cause))
				.withCause(cause);
		assertThat(Thread.currentThread().isInterrupted()).isTrue();
	}
}
