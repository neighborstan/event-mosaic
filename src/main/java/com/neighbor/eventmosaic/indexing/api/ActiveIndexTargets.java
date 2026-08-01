package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.UUID;

/** Exact writable Event/Mention pair одной ACTIVE generation. */
public record ActiveIndexTargets(
		String partitionKey,
		long partitionStateVersion,
		long generationId,
		UUID generationUuid,
		ExactIndexTarget event,
		ExactIndexTarget mention
) {

	/** Проверяет coherent partition/generation binding и kind пары. */
	public ActiveIndexTargets {
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		Objects.requireNonNull(generationUuid, "generationUuid must not be null");
		Objects.requireNonNull(event, "event must not be null");
		Objects.requireNonNull(mention, "mention must not be null");
		if (partitionKey.isBlank()) {
			throw new IllegalArgumentException("partitionKey must not be blank");
		}
		if (partitionStateVersion <= 0) {
			throw new IllegalArgumentException("partitionStateVersion must be positive");
		}
		if (generationId <= 0) {
			throw new IllegalArgumentException("generationId must be positive");
		}
		if (!event.indexName().startsWith("gdelt-events-v1-")) {
			throw new IllegalArgumentException("event target must be an Event index");
		}
		if (!mention.indexName().startsWith("gdelt-mentions-v1-")) {
			throw new IllegalArgumentException("mention target must be a Mention index");
		}
		String eventIdentity = event.indexName().substring("gdelt-events-v1-".length());
		String mentionIdentity = mention.indexName().substring("gdelt-mentions-v1-".length());
		if (!eventIdentity.equals(mentionIdentity)
				|| !eventIdentity.startsWith(partitionKey + "-g")) {
			throw new IllegalArgumentException(
					"Exact targets must be one coherent partition/generation pair");
		}
	}

	/** Возвращает kind-specific target без alias и wildcard. */
	public ExactIndexTarget target(GdeltIndexKind kind) {
		return switch (Objects.requireNonNull(kind, "kind must not be null")) {
			case EVENT -> event;
			case MENTION -> mention;
		};
	}
}
