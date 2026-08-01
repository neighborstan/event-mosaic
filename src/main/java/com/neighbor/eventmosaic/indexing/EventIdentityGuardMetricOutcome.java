package com.neighbor.eventmosaic.indexing;

/** Low-cardinality outcome одной bounded Event identity guard проверки. */
enum EventIdentityGuardMetricOutcome {

	ABSENT,
	REPLAY,
	CONFLICT,
	FAILURE
}
