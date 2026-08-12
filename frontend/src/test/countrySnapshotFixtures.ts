import {
  joinCountrySnapshot,
  validateCountrySnapshot,
  type CountrySnapshot,
  type JoinedCountrySnapshot,
} from "../features/country-map/api/countrySnapshot";
import { createValidatedCountryGeometry } from "./countryGeometryFixtures";

export interface MutableCountrySnapshotDocument {
  snapshot: {
    from: string;
    to: string;
    geometryVersion: string;
    toneModelVersion: string;
  };
  coverage: {
    status: string;
    missingIntervals: { from: string; to: string }[] | null;
  };
  quality: {
    eligibleEventCount: number;
    mappedEventCount: number;
    unlocatedEventCount: number;
    unmappedEventCount: number;
    unlocatedReasonCounts: { reason: string; eventCount: number }[];
    unmappedReasonCounts: { reason: string; eventCount: number }[];
  };
  regions: MutableCountrySnapshotRegion[];
}

export interface MutableCountrySnapshotRegion {
  regionId: string;
  eventCount: number;
  coloredEventCount: number;
  missingToneEventCount: number;
  toneCounts: {
    NEGATIVE_EXTREME: number;
    NEGATIVE_STRONG: number;
    NEGATIVE_MILD: number;
    ZERO: number;
    POSITIVE_MILD: number;
    POSITIVE_STRONG: number;
    POSITIVE_EXTREME: number;
  };
}

export function createCountrySnapshotDocument(): MutableCountrySnapshotDocument {
  const regionIds = createValidatedCountryGeometry()
    .features.map((feature) => feature.properties.regionId)
    .slice()
    .sort();
  const regions = regionIds.map((regionId, index) =>
    index === 0 ? populatedRegion(regionId) : emptyRegion(regionId),
  );

  return {
    snapshot: {
      from: "2026-08-10T12:15:00Z",
      to: "2026-08-11T12:15:00Z",
      geometryVersion: "country-v1",
      toneModelVersion: "tone-bands-v1",
    },
    coverage: {
      status: "COMPLETE",
      missingIntervals: [],
    },
    quality: {
      eligibleEventCount: 13,
      mappedEventCount: 4,
      unlocatedEventCount: 3,
      unmappedEventCount: 6,
      unlocatedReasonCounts: [
        { reason: "ACTION_GEO_MISSING_OR_INVALID", eventCount: 1 },
        { reason: "ACTOR_FALLBACK", eventCount: 2 },
        { reason: "OTHER", eventCount: 0 },
      ],
      unmappedReasonCounts: [
        { reason: "COUNTRY_CODE_MISSING", eventCount: 1 },
        { reason: "UNKNOWN_COUNTRY_CODE", eventCount: 1 },
        { reason: "NO_REGION_GEOMETRY", eventCount: 1 },
        { reason: "AMBIGUOUS_REGION_MAPPING", eventCount: 1 },
        { reason: "UNSUPPORTED_COUNTRY_CODE", eventCount: 1 },
        { reason: "OTHER", eventCount: 1 },
      ],
    },
    regions,
  };
}

export function createValidatedCountrySnapshot(): CountrySnapshot {
  return validateCountrySnapshot(createCountrySnapshotDocument());
}

export function createJoinedCountrySnapshot(): JoinedCountrySnapshot {
  return joinCountrySnapshot(
    createValidatedCountrySnapshot(),
    createValidatedCountryGeometry(),
  );
}

function populatedRegion(regionId: string): MutableCountrySnapshotRegion {
  return {
    regionId,
    eventCount: 4,
    coloredEventCount: 3,
    missingToneEventCount: 1,
    toneCounts: {
      NEGATIVE_EXTREME: 1,
      NEGATIVE_STRONG: 0,
      NEGATIVE_MILD: 0,
      ZERO: 1,
      POSITIVE_MILD: 0,
      POSITIVE_STRONG: 0,
      POSITIVE_EXTREME: 1,
    },
  };
}

function emptyRegion(regionId: string): MutableCountrySnapshotRegion {
  return {
    regionId,
    eventCount: 0,
    coloredEventCount: 0,
    missingToneEventCount: 0,
    toneCounts: {
      NEGATIVE_EXTREME: 0,
      NEGATIVE_STRONG: 0,
      NEGATIVE_MILD: 0,
      ZERO: 0,
      POSITIVE_MILD: 0,
      POSITIVE_STRONG: 0,
      POSITIVE_EXTREME: 0,
    },
  };
}
