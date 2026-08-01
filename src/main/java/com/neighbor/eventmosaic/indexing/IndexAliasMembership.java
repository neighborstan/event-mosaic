package com.neighbor.eventmosaic.indexing;

import java.util.Objects;
import java.util.Set;

/** Exact index memberships двух stable read aliases. */
record IndexAliasMembership(Set<String> eventIndices, Set<String> mentionIndices) {

	IndexAliasMembership {
		eventIndices = Set.copyOf(Objects.requireNonNull(
				eventIndices, "eventIndices must not be null"));
		mentionIndices = Set.copyOf(Objects.requireNonNull(
				mentionIndices, "mentionIndices must not be null"));
	}

	static IndexAliasMembership empty() {
		return new IndexAliasMembership(Set.of(), Set.of());
	}
}
