package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;

/**
 * Представляет результат conditional transition вместе с owning run.
 */
record ArchiveTransition(long runId, AttemptTransitionResult result) {
}
