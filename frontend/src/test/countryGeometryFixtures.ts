import {
  validateCountryGeometry,
  type CountryGeometry,
} from "../features/country-map/api/countryGeometry";

interface MutableCountryGeometryProperties {
  regionId: string;
  displayName: string;
  disputeStatus: string;
  geometrySource: string;
  unexpectedProperty?: string;
}

interface MutableCountryGeometryShape {
  type: string;
  coordinates: unknown;
}

export interface MutableCountryGeometryFeature {
  type: string;
  properties: MutableCountryGeometryProperties;
  geometry: MutableCountryGeometryShape;
}

export interface MutableCountryGeometryDocument {
  type: string;
  geometryVersion: string;
  features: MutableCountryGeometryFeature[];
}

export function createCountryGeometryDocument(
  featureCount = 258,
): MutableCountryGeometryDocument {
  return {
    type: "FeatureCollection",
    geometryVersion: "country-v1",
    features: Array.from({ length: featureCount }, (_, index) =>
      createCountryFeature(index),
    ),
  };
}

export function createValidatedCountryGeometry(): CountryGeometry {
  return validateCountryGeometry(createCountryGeometryDocument());
}

function createCountryFeature(index: number): MutableCountryGeometryFeature {
  const longitude = (index % 120) - 60;
  const latitude = (index % 60) - 30;
  const ring = [
    [longitude, latitude],
    [longitude + 0.25, latitude],
    [longitude + 0.25, latitude + 0.25],
    [longitude, latitude],
  ];

  return {
    type: "Feature",
    properties: {
      regionId: `country:r${index.toString(36)}`,
      displayName: `Region ${index}`,
      disputeStatus: index === 1 ? "DISPUTED_DE_FACTO" : "STANDARD",
      geometrySource: "natural-earth-10m",
    },
    geometry:
      index === 1
        ? {
            type: "MultiPolygon",
            coordinates: [[ring]],
          }
        : {
            type: "Polygon",
            coordinates: [ring],
          },
  };
}
