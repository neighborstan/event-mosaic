package com.neighbor.eventmosaic.ingestion.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Контракт ошибок ручной очистки поколения")
class GenerationCleanupErrorContractTest {

	@Test
	@DisplayName("Конфликт evidence требует нового плана и не помечается временным отказом")
	void rejectedCleanupUsesNonRetryableBranch() {
		var exception = new GenerationCleanupRejectedException(
				GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);

		assertThat(exception).isInstanceOf(NonRetryableException.class);
		assertThat(exception.errorCode())
				.isEqualTo(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
	}

	@Test
	@DisplayName("Временная недоступность зависимости допускает повтор после проверки")
	void unavailableCleanupUsesRetryableBranch() {
		var exception = new GenerationCleanupUnavailableException(
				GenerationCleanupErrorCode.CLEANUP_OPERATION_UNAVAILABLE);

		assertThat(exception).isInstanceOf(RetryableException.class);
		assertThat(exception.errorCode())
				.isEqualTo(GenerationCleanupErrorCode.CLEANUP_OPERATION_UNAVAILABLE);
	}
}
