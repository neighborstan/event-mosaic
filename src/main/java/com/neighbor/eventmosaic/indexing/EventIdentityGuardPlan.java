package com.neighbor.eventmosaic.indexing;

import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Immutable результат bounded Event identity guard для одной bulk-порции.
 *
 * @param documentsToCreate уникальные документы, отсутствующие в current target
 * @param replayPositions zero-based позиции logical replay исходной порции
 */
record EventIdentityGuardPlan(
		List<IndexedEventDocument> documentsToCreate,
		List<Integer> replayPositions
) {

	EventIdentityGuardPlan {
		documentsToCreate = List.copyOf(Objects.requireNonNull(
				documentsToCreate,
				"documentsToCreate must not be null"));
		replayPositions = List.copyOf(Objects.requireNonNull(
				replayPositions,
				"replayPositions must not be null"));
		if (new HashSet<>(replayPositions).size() != replayPositions.size()
				|| replayPositions.stream().anyMatch(position -> position == null || position < 0)) {
			throw new IllegalArgumentException(
					"replayPositions must contain unique non-negative positions");
		}
	}

	/**
	 * Возвращает число logical replay в исходной порции.
	 *
	 * @return число пропущенных exact provenance records
	 */
	int replayCount() {
		return replayPositions.size();
	}
}
