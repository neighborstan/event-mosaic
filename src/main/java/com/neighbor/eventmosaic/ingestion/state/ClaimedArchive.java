package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;

/**
 * Представляет результат archive claim вместе с owning run.
 */
record ClaimedArchive(long runId, ArchiveAttempt attempt) {
}
