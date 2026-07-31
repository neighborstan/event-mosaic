package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.UUID;

/**
 * Metadata exact Event/Mention physical generation.
 *
 * @param id database identity
 * @param generationUuid durable generation identity
 * @param partitionKey owning logical partition
 * @param generationNumber monotonic number внутри partition
 * @param status lifecycle state
 * @param names exact physical names
 * @param eventIndexUuid observed Elasticsearch Event UUID либо {@code null}
 * @param mentionIndexUuid observed Elasticsearch Mention UUID либо {@code null}
 * @param stateVersion monotonic generation state version
 */
public record IndexGeneration(
		long id,
		UUID generationUuid,
		String partitionKey,
		int generationNumber,
		IndexGenerationStatus status,
		IndexGenerationNames names,
		String eventIndexUuid,
		String mentionIndexUuid,
		long stateVersion
) {

	/** Проверяет identity и обязательную UUID-пару для читаемых состояний. */
	public IndexGeneration {
		if (id <= 0) {
			throw new IllegalArgumentException("id must be positive");
		}
		Objects.requireNonNull(generationUuid, "generationUuid must not be null");
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		if (partitionKey.isBlank()) {
			throw new IllegalArgumentException("partitionKey must not be blank");
		}
		if (generationNumber <= 0) {
			throw new IllegalArgumentException("generationNumber must be positive");
		}
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(names, "names must not be null");
		boolean completePairRequired = status == IndexGenerationStatus.ACTIVE
				|| status == IndexGenerationStatus.SUPERSEDED;
		if (completePairRequired && (eventIndexUuid == null || mentionIndexUuid == null)) {
			throw new IllegalArgumentException("ACTIVE and SUPERSEDED states require both index UUIDs");
		}
		if ((eventIndexUuid != null && eventIndexUuid.isBlank())
				|| (mentionIndexUuid != null && mentionIndexUuid.isBlank())) {
			throw new IllegalArgumentException("Present index UUID must not be blank");
		}
		if (stateVersion < 0) {
			throw new IllegalArgumentException("stateVersion must not be negative");
		}
	}
}
