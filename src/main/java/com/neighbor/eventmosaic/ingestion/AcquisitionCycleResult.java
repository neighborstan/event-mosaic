package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import java.util.Objects;
import java.util.Optional;

/** Внутренний итог acquisition pass и состояния source poll. */
record AcquisitionCycleResult(
		Optional<IngestionRunState> runState,
		boolean sourcePollDeferred
) {

	AcquisitionCycleResult {
		Objects.requireNonNull(runState, "runState must not be null");
	}
}
