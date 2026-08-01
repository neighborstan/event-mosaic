package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Captured exact physical target одной processing attempt.
 *
 * @param indexKind вид GDELT read-model document
 * @param partitionKey owning logical partition
 * @param partitionStateVersion fencing version partition при claim
 * @param generationId database identity ACTIVE generation
 * @param generationUuid durable identity ACTIVE generation
 * @param indexName exact physical index name выбранного kind
 * @param indexUuid observed Elasticsearch UUID выбранного index
 */
public record ArchiveProcessingTargetBinding(
		GdeltIndexKind indexKind,
		String partitionKey,
		long partitionStateVersion,
		long generationId,
		UUID generationUuid,
		String indexName,
		String indexUuid
) {
	private static final Pattern PARTITION_KEY = Pattern.compile(
			"^[a-z0-9][a-z0-9_-]{0,31}$");

	/** Проверяет exact identity и запрещает wildcard target. */
	public ArchiveProcessingTargetBinding {
		Objects.requireNonNull(indexKind, "indexKind must not be null");
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		if (!PARTITION_KEY.matcher(partitionKey).matches()) {
			throw new IllegalArgumentException("partitionKey must be a bounded lowercase key");
		}
		if (partitionStateVersion < 0) {
			throw new IllegalArgumentException("partitionStateVersion must not be negative");
		}
		if (generationId <= 0) {
			throw new IllegalArgumentException("generationId must be positive");
		}
		Objects.requireNonNull(generationUuid, "generationUuid must not be null");
		requireExactIndexName(indexKind, partitionKey, indexName);
		requireExactValue(indexUuid, "indexUuid");
	}

	private static void requireExactIndexName(
			GdeltIndexKind indexKind,
			String partitionKey,
			String indexName
	) {
		requireExactValue(indexName, "indexName");
		String expectedPrefix = indexKind.indexName() + "-" + partitionKey + "-g";
		if (!indexName.startsWith(expectedPrefix)
				|| !indexName.substring(expectedPrefix.length()).matches("[0-9]+")) {
			throw new IllegalArgumentException(
					"indexName must be an exact physical index of the bound kind and partition");
		}
	}

	private static void requireExactValue(String value, String field) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.isBlank()
				|| value.length() > 255
				|| value.indexOf('*') >= 0
				|| value.indexOf('?') >= 0
				|| value.indexOf(',') >= 0
				|| value.chars().anyMatch(Character::isWhitespace)) {
			throw new IllegalArgumentException(field + " must be a non-wildcard exact value");
		}
	}
}
