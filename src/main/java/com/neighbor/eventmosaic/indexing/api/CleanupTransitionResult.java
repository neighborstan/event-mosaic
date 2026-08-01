package com.neighbor.eventmosaic.indexing.api;

/** Результат fenced cleanup transition без неявного перехвата ownership. */
public enum CleanupTransitionResult {
	APPLIED,
	OWNERSHIP_LOST
}
