package com.neighbor.eventmosaic.processing;

/**
 * Ограниченные причины локального processing mapping rejection.
 */
enum ProcessingMappingRejection {
	EVENT_ID_INVALID,
	EVENT_DAY_MISSING,
	EVENT_DATE_ADDED_MISSING,
	MENTION_ID_INVALID,
	MENTION_EVENT_TIME_MISSING,
	MENTION_TIME_MISSING,
	MENTION_TYPE_MISSING,
	MENTION_IDENTIFIER_MISSING
}
