package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import java.util.Objects;

/**
 * Один раз выбирает Event location в порядке Action, Actor1, Actor2.
 */
final class EventGeoSelector {

	Selection select(GdeltEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		int invalidCandidates = 0;

		Candidate action = evaluate(new GeoCandidate(
				IndexedLocationRole.ACTION,
				new GeoAttributes(
						event.actionGeoFullName(),
						event.actionGeoCountryCode(),
						event.actionGeoAdm1Code(),
						event.actionGeoAdm2Code(),
						event.actionGeoFeatureId()),
				new Coordinates(event.actionGeoLat(), event.actionGeoLong())));
		if (action.location() != null) {
			return new Selection(action.location(), invalidCandidates);
		}
		invalidCandidates += action.invalid() ? 1 : 0;

		Candidate actor1 = evaluate(new GeoCandidate(
				IndexedLocationRole.ACTOR1,
				new GeoAttributes(
						event.actor1GeoFullName(),
						event.actor1GeoCountryCode(),
						event.actor1GeoAdm1Code(),
						event.actor1GeoAdm2Code(),
						event.actor1GeoFeatureId()),
				new Coordinates(event.actor1GeoLat(), event.actor1GeoLong())));
		if (actor1.location() != null) {
			return new Selection(actor1.location(), invalidCandidates);
		}
		invalidCandidates += actor1.invalid() ? 1 : 0;

		Candidate actor2 = evaluate(new GeoCandidate(
				IndexedLocationRole.ACTOR2,
				new GeoAttributes(
						event.actor2GeoFullName(),
						event.actor2GeoCountryCode(),
						event.actor2GeoAdm1Code(),
						event.actor2GeoAdm2Code(),
						event.actor2GeoFeatureId()),
				new Coordinates(event.actor2GeoLat(), event.actor2GeoLong())));
		if (actor2.location() != null) {
			return new Selection(actor2.location(), invalidCandidates);
		}
		invalidCandidates += actor2.invalid() ? 1 : 0;
		return new Selection(null, invalidCandidates);
	}

	private static Candidate evaluate(GeoCandidate candidate) {
		Coordinates coordinates = candidate.coordinates();
		Double latitude = coordinates.latitude();
		Double longitude = coordinates.longitude();
		if (latitude == null && longitude == null) {
			return Candidate.absent();
		}
		if (latitude == null
				|| longitude == null
				|| !Double.isFinite(latitude)
				|| !Double.isFinite(longitude)
				|| latitude < -90.0
				|| latitude > 90.0
				|| longitude < -180.0
				|| longitude > 180.0) {
			return Candidate.invalidCandidate();
		}
		GeoAttributes attributes = candidate.attributes();
		return Candidate.valid(new IndexedEventLocation(
				candidate.role(),
				attributes.name(),
				attributes.countryCode(),
				attributes.admin1Code(),
				attributes.admin2Code(),
				attributes.featureId(),
				new IndexedGeoPoint(latitude, longitude)));
	}

	private record GeoCandidate(
			IndexedLocationRole role,
			GeoAttributes attributes,
			Coordinates coordinates
	) {
	}

	private record GeoAttributes(
			String name,
			String countryCode,
			String admin1Code,
			String admin2Code,
			String featureId
	) {
	}

	private record Coordinates(Double latitude, Double longitude) {
	}

	/**
	 * Результат выбора с bounded diagnostic count.
	 *
	 * @param location выбранная location или {@code null}
	 * @param invalidCandidateCount число встретившихся invalid candidates
	 */
	record Selection(IndexedEventLocation location, int invalidCandidateCount) {

		Selection {
			if (invalidCandidateCount < 0) {
				throw new IllegalArgumentException(
						"invalidCandidateCount must not be negative");
			}
		}
	}

	private record Candidate(IndexedEventLocation location, boolean invalid) {

		private static Candidate absent() {
			return new Candidate(null, false);
		}

		private static Candidate invalidCandidate() {
			return new Candidate(null, true);
		}

		private static Candidate valid(IndexedEventLocation location) {
			return new Candidate(Objects.requireNonNull(location), false);
		}
	}
}
