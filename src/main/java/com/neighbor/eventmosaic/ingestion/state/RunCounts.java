package com.neighbor.eventmosaic.ingestion.state;

/**
 * Хранит низкоуровневые counts, из которых чистая policy выводит run status.
 */
record RunCounts(int staged, int processing, int failed, int attempted) {
}
