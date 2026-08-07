package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.observability.StoragePressureState;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.util.Objects;

/**
 * Останавливает новую storage-growing operation, не изменяя retry ledger и не
 * запуская автоматическое удаление истории.
 */
public final class StoragePressureException extends RetryableException
		implements IngestionFailureContract {

	private final StorageResource resource;
	private final StoragePressureState pressureState;
	private final IngestionFailure failure = new IngestionFailure(
			IngestionErrorCode.STORAGE_PRESSURE,
			true);

	/** Создает bounded operational отказ для одного storage ресурса. */
	public StoragePressureException(
			StorageResource resource,
			StoragePressureState pressureState
	) {
		super(IngestionErrorCode.STORAGE_PRESSURE);
		this.resource = Objects.requireNonNull(resource, "resource must not be null");
		this.pressureState = Objects.requireNonNull(
				pressureState,
				"pressureState must not be null");
		if (pressureState == StoragePressureState.AVAILABLE) {
			throw new IllegalArgumentException("available storage must not be rejected");
		}
	}

	/** Возвращает ресурс с дефицитом или неподтвержденной доступностью. */
	public StorageResource resource() {
		return resource;
	}

	/** Возвращает bounded результат проверки. */
	public StoragePressureState pressureState() {
		return pressureState;
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}
}
