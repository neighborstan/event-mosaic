package com.neighbor.eventmosaic.ingestion.trigger;

/** Состояние одного локального automatic ingestion scheduler. */
enum AutomaticIngestionSchedulerState {
	STARTING,
	RUNNING,
	STOPPING,
	STOPPED
}
