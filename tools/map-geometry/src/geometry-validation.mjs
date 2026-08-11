const GEOMETRY_TYPES = new Set(["Polygon", "MultiPolygon"]);

export function canonicalizeEngineFeatureCollection({
  featureCollection,
  geometryVersion,
  geometrySource,
  regions,
  coordinatePrecision,
}) {
  assertStrictObject(featureCollection, "mapshaperOutput", ["type", "features"]);
  assertExact(featureCollection.type, "FeatureCollection", "mapshaperOutput.type");
  assertNonEmptyArray(featureCollection.features, "mapshaperOutput.features");

  const regionById = new Map(regions.map((region) => [region.regionId, region]));
  const seenRegionIds = new Set();
  const features = featureCollection.features.map((feature, index) => {
    const path = `mapshaperOutput.features[${index}]`;
    assertStrictObject(feature, path, ["type", "properties", "geometry"]);
    assertExact(feature.type, "Feature", `${path}.type`);
    assertStrictObject(feature.properties, `${path}.properties`, ["regionId"]);
    const region = regionById.get(feature.properties.regionId);
    if (region === undefined) {
      fail(`${path}.properties.regionId`, "regionId отсутствует в country crosswalk");
    }
    assertUniqueRegionId(seenRegionIds, region.regionId, `${path}.properties.regionId`);
    return buildPublicFeature(
      region,
      geometrySource,
      canonicalizeGeometry(
        feature.geometry,
        `${path}.geometry`,
        coordinatePrecision,
      ),
    );
  });
  assertCompleteRegionCoverage(seenRegionIds, regionById);
  features.sort((left, right) => compareStrings(
    left.properties.regionId,
    right.properties.regionId,
  ));

  return {
    type: "FeatureCollection",
    geometryVersion,
    features,
  };
}

export function validateCountryGeoJson({
  featureCollection,
  geometryVersion,
  geometrySource,
  regions,
  coordinatePrecision,
}) {
  assertStrictObject(featureCollection, "countriesGeojson", [
    "type",
    "geometryVersion",
    "features",
  ]);
  assertExact(featureCollection.type, "FeatureCollection", "countriesGeojson.type");
  assertExact(
    featureCollection.geometryVersion,
    geometryVersion,
    "countriesGeojson.geometryVersion",
  );
  assertNonEmptyArray(featureCollection.features, "countriesGeojson.features");

  const regionById = new Map(regions.map((region) => [region.regionId, region]));
  const seenRegionIds = new Set();
  const canonicalFeatures = featureCollection.features.map((feature, index) => {
    const path = `countriesGeojson.features[${index}]`;
    assertStrictObject(feature, path, ["type", "properties", "geometry"]);
    assertExact(feature.type, "Feature", `${path}.type`);
    assertStrictObject(feature.properties, `${path}.properties`, [
      "regionId",
      "displayName",
      "disputeStatus",
      "geometrySource",
    ]);
    const region = regionById.get(feature.properties.regionId);
    if (region === undefined) {
      fail(`${path}.properties.regionId`, "regionId отсутствует в runtime catalog");
    }
    assertUniqueRegionId(seenRegionIds, region.regionId, `${path}.properties.regionId`);
    assertExact(feature.properties.displayName, region.displayName, `${path}.properties.displayName`);
    assertExact(
      feature.properties.disputeStatus,
      region.disputeStatus,
      `${path}.properties.disputeStatus`,
    );
    assertExact(
      feature.properties.geometrySource,
      geometrySource,
      `${path}.properties.geometrySource`,
    );
    return buildPublicFeature(
      region,
      geometrySource,
      canonicalizeGeometry(
        feature.geometry,
        `${path}.geometry`,
        coordinatePrecision,
      ),
    );
  });
  assertCompleteRegionCoverage(seenRegionIds, regionById);
  canonicalFeatures.sort((left, right) => compareStrings(
    left.properties.regionId,
    right.properties.regionId,
  ));

  const canonicalValue = {
    type: "FeatureCollection",
    geometryVersion,
    features: canonicalFeatures,
  };
  return {
    canonicalValue,
    featureCount: canonicalFeatures.length,
    vertexCount: countVertices(canonicalValue),
  };
}

export function canonicalizeGeometry(
  geometry,
  path = "geometry",
  coordinatePrecision = undefined,
) {
  assertStrictObject(geometry, path, ["type", "coordinates"]);
  if (!GEOMETRY_TYPES.has(geometry.type)) {
    fail(`${path}.type`, 'допустимы только "Polygon" и "MultiPolygon"');
  }

  if (geometry.type === "Polygon") {
    return {
      type: "Polygon",
      coordinates: canonicalizePolygon(
        geometry.coordinates,
        `${path}.coordinates`,
        coordinatePrecision,
      ),
    };
  }

  assertNonEmptyArray(geometry.coordinates, `${path}.coordinates`);
  const polygons = geometry.coordinates.map((polygon, index) =>
    canonicalizePolygon(
      polygon,
      `${path}.coordinates[${index}]`,
      coordinatePrecision,
    ),
  );
  polygons.sort(compareCoordinateTrees);
  return { type: "MultiPolygon", coordinates: polygons };
}

export function countVertices(featureCollection) {
  let count = 0;
  for (const feature of featureCollection.features) {
    visitPositions(feature.geometry.coordinates, () => {
      count += 1;
    });
  }
  return count;
}

function buildPublicFeature(region, geometrySource, geometry) {
  return {
    type: "Feature",
    properties: {
      regionId: region.regionId,
      displayName: region.displayName,
      disputeStatus: region.disputeStatus,
      geometrySource,
    },
    geometry,
  };
}

function canonicalizePolygon(coordinates, path, coordinatePrecision) {
  assertNonEmptyArray(coordinates, path);
  const outerRing = canonicalizeRing(
    coordinates[0],
    `${path}[0]`,
    coordinatePrecision,
  );
  const holes = coordinates
    .slice(1)
    .map((ring, index) => canonicalizeRing(
      ring,
      `${path}[${index + 1}]`,
      coordinatePrecision,
    ));
  holes.sort(compareCoordinateTrees);
  if (coordinatePrecision !== undefined) {
    assertValidHoleTopology(outerRing, holes, path, coordinatePrecision);
  }
  return [outerRing, ...holes];
}

function canonicalizeRing(ring, path, coordinatePrecision) {
  if (!Array.isArray(ring) || ring.length < 4) {
    fail(path, "ring должен содержать минимум четыре positions");
  }
  const positions = ring.map((position, index) =>
    validatePosition(position, `${path}[${index}]`, coordinatePrecision),
  );
  if (!positionsEqual(positions[0], positions.at(-1))) {
    fail(path, "ring должен быть замкнут одинаковыми первой и последней positions");
  }

  const openRing = positions.slice(0, -1);
  const distinctPositions = new Set(openRing.map(([longitude, latitude]) =>
    `${longitude},${latitude}`,
  ));
  if (distinctPositions.size < 3) {
    fail(path, "ring должен содержать минимум три различные positions");
  }
  if (signedArea(openRing) === 0) {
    fail(path, "ring имеет нулевую площадь");
  }
  if (coordinatePrecision !== undefined) {
    assertSimpleRing(openRing, path, coordinatePrecision);
  }

  const startIndex = findLexicographicallySmallestRotation(openRing);
  const rotated = [
    ...openRing.slice(startIndex),
    ...openRing.slice(0, startIndex),
  ];
  return [...rotated, [...rotated[0]]];
}

function validatePosition(position, path, coordinatePrecision) {
  if (!Array.isArray(position) || position.length !== 2) {
    fail(path, "ожидается двумерная position [longitude, latitude]");
  }
  const [rawLongitude, rawLatitude] = position;
  if (!Number.isFinite(rawLongitude) || rawLongitude < -180 || rawLongitude > 180) {
    fail(`${path}[0]`, "longitude должен быть конечным числом в диапазоне [-180, 180]");
  }
  if (!Number.isFinite(rawLatitude) || rawLatitude < -90 || rawLatitude > 90) {
    fail(`${path}[1]`, "latitude должен быть конечным числом в диапазоне [-90, 90]");
  }
  if (coordinatePrecision === undefined) {
    return [normalizeNegativeZero(rawLongitude), normalizeNegativeZero(rawLatitude)];
  }
  const precisionScale = getPrecisionScale(coordinatePrecision);
  return [
    quantizeCoordinate(rawLongitude, precisionScale, `${path}[0]`),
    quantizeCoordinate(rawLatitude, precisionScale, `${path}[1]`),
  ];
}

function getPrecisionScale(coordinatePrecision) {
  if (!Number.isFinite(coordinatePrecision) || coordinatePrecision <= 0) {
    throw new TypeError("coordinatePrecision должен быть положительным конечным числом");
  }
  const precisionScale = Math.round(1 / coordinatePrecision);
  const reciprocalError = Math.abs(
    precisionScale * coordinatePrecision - 1,
  );
  if (
    !Number.isSafeInteger(precisionScale) ||
    precisionScale <= 0 ||
    precisionScale > 100000 ||
    reciprocalError > Number.EPSILON * 8
  ) {
    throw new TypeError(
      "coordinatePrecision должен задавать десятичную сетку не точнее 0.00001",
    );
  }
  return precisionScale;
}

function quantizeCoordinate(value, precisionScale, path) {
  const scaled = value * precisionScale;
  const nearestGridValue = Math.round(scaled);
  const tolerance = Number.EPSILON * Math.max(1, Math.abs(scaled)) * 16;
  if (Math.abs(scaled - nearestGridValue) > tolerance) {
    fail(path, "координата не лежит на зафиксированной precision grid");
  }
  return normalizeNegativeZero(nearestGridValue / precisionScale);
}

function assertSimpleRing(openRing, path, coordinatePrecision) {
  const precisionScale = getPrecisionScale(coordinatePrecision);
  const gridRing = toGridRing(openRing, precisionScale);
  for (let leftIndex = 0; leftIndex < gridRing.length; leftIndex += 1) {
    const leftStart = gridRing[leftIndex];
    const leftEnd = gridRing[(leftIndex + 1) % gridRing.length];
    for (let rightIndex = leftIndex + 1; rightIndex < gridRing.length; rightIndex += 1) {
      if (
        rightIndex === leftIndex + 1 ||
        (leftIndex === 0 && rightIndex === gridRing.length - 1)
      ) {
        continue;
      }
      const rightStart = gridRing[rightIndex];
      const rightEnd = gridRing[(rightIndex + 1) % gridRing.length];
      if (
        boundingBoxesOverlap(leftStart, leftEnd, rightStart, rightEnd) &&
        segmentsIntersect(leftStart, leftEnd, rightStart, rightEnd)
      ) {
        fail(
          path,
          `ring самопересекается между segments ${leftIndex} и ${rightIndex}`,
        );
      }
    }
  }
}

function assertValidHoleTopology(
  outerRing,
  holes,
  path,
  coordinatePrecision,
) {
  const precisionScale = getPrecisionScale(coordinatePrecision);
  const outerGridRing = toGridRing(outerRing.slice(0, -1), precisionScale);
  const holeGridRings = holes.map((hole) =>
    toGridRing(hole.slice(0, -1), precisionScale));

  for (let holeIndex = 0; holeIndex < holeGridRings.length; holeIndex += 1) {
    const holePath = `${path}[${holeIndex + 1}]`;
    const hole = holeGridRings[holeIndex];
    if (ringsIntersect(outerGridRing, hole)) {
      fail(holePath, "hole пересекает exterior ring");
    }
    if (classifyPointInRing(hole[0], outerGridRing) !== 1) {
      fail(holePath, "hole должен лежать строго внутри exterior ring");
    }

    for (let previousIndex = 0; previousIndex < holeIndex; previousIndex += 1) {
      const previousHole = holeGridRings[previousIndex];
      if (
        ringsIntersect(previousHole, hole) ||
        classifyPointInRing(hole[0], previousHole) !== -1 ||
        classifyPointInRing(previousHole[0], hole) !== -1
      ) {
        fail(
          holePath,
          `hole пересекается или вложен в hole ${previousIndex + 1}`,
        );
      }
    }
  }
}

function toGridRing(ring, precisionScale) {
  return ring.map(([longitude, latitude]) => [
    Math.round(longitude * precisionScale),
    Math.round(latitude * precisionScale),
  ]);
}

function ringsIntersect(leftRing, rightRing) {
  const leftBounds = ringBounds(leftRing);
  const rightBounds = ringBounds(rightRing);
  if (!boundsOverlap(leftBounds, rightBounds)) {
    return false;
  }
  for (let leftIndex = 0; leftIndex < leftRing.length; leftIndex += 1) {
    const leftStart = leftRing[leftIndex];
    const leftEnd = leftRing[(leftIndex + 1) % leftRing.length];
    for (let rightIndex = 0; rightIndex < rightRing.length; rightIndex += 1) {
      const rightStart = rightRing[rightIndex];
      const rightEnd = rightRing[(rightIndex + 1) % rightRing.length];
      if (
        boundingBoxesOverlap(leftStart, leftEnd, rightStart, rightEnd) &&
        segmentsIntersect(leftStart, leftEnd, rightStart, rightEnd)
      ) {
        return true;
      }
    }
  }
  return false;
}

function classifyPointInRing(point, ring) {
  let inside = false;
  for (let index = 0; index < ring.length; index += 1) {
    const start = ring[index];
    const end = ring[(index + 1) % ring.length];
    if (orientation(start, end, point) === 0 && pointOnSegment(point, start, end)) {
      return 0;
    }
    const crossesLatitude = (start[1] > point[1]) !== (end[1] > point[1]);
    if (crossesLatitude) {
      const intersectionLongitude =
        start[0] +
        ((end[0] - start[0]) * (point[1] - start[1])) /
          (end[1] - start[1]);
      if (intersectionLongitude > point[0]) {
        inside = !inside;
      }
    }
  }
  return inside ? 1 : -1;
}

function ringBounds(ring) {
  let minimumLongitude = Infinity;
  let minimumLatitude = Infinity;
  let maximumLongitude = -Infinity;
  let maximumLatitude = -Infinity;
  for (const [longitude, latitude] of ring) {
    minimumLongitude = Math.min(minimumLongitude, longitude);
    minimumLatitude = Math.min(minimumLatitude, latitude);
    maximumLongitude = Math.max(maximumLongitude, longitude);
    maximumLatitude = Math.max(maximumLatitude, latitude);
  }
  return [
    minimumLongitude,
    minimumLatitude,
    maximumLongitude,
    maximumLatitude,
  ];
}

function boundsOverlap(left, right) {
  return (
    left[0] <= right[2] &&
    right[0] <= left[2] &&
    left[1] <= right[3] &&
    right[1] <= left[3]
  );
}

function boundingBoxesOverlap(leftStart, leftEnd, rightStart, rightEnd) {
  return (
    Math.min(leftStart[0], leftEnd[0]) <= Math.max(rightStart[0], rightEnd[0]) &&
    Math.min(rightStart[0], rightEnd[0]) <= Math.max(leftStart[0], leftEnd[0]) &&
    Math.min(leftStart[1], leftEnd[1]) <= Math.max(rightStart[1], rightEnd[1]) &&
    Math.min(rightStart[1], rightEnd[1]) <= Math.max(leftStart[1], leftEnd[1])
  );
}

function segmentsIntersect(leftStart, leftEnd, rightStart, rightEnd) {
  const first = orientation(leftStart, leftEnd, rightStart);
  const second = orientation(leftStart, leftEnd, rightEnd);
  const third = orientation(rightStart, rightEnd, leftStart);
  const fourth = orientation(rightStart, rightEnd, leftEnd);
  if (
    first !== 0 &&
    second !== 0 &&
    third !== 0 &&
    fourth !== 0
  ) {
    return Math.sign(first) !== Math.sign(second) && Math.sign(third) !== Math.sign(fourth);
  }
  return (
    (first === 0 && pointOnSegment(rightStart, leftStart, leftEnd)) ||
    (second === 0 && pointOnSegment(rightEnd, leftStart, leftEnd)) ||
    (third === 0 && pointOnSegment(leftStart, rightStart, rightEnd)) ||
    (fourth === 0 && pointOnSegment(leftEnd, rightStart, rightEnd))
  );
}

function orientation(start, end, point) {
  return (
    (end[0] - start[0]) * (point[1] - start[1]) -
    (end[1] - start[1]) * (point[0] - start[0])
  );
}

function pointOnSegment(point, start, end) {
  return (
    point[0] >= Math.min(start[0], end[0]) &&
    point[0] <= Math.max(start[0], end[0]) &&
    point[1] >= Math.min(start[1], end[1]) &&
    point[1] <= Math.max(start[1], end[1])
  );
}

function findLexicographicallySmallestRotation(positions) {
  let smallestIndex = 0;
  for (let candidateIndex = 1; candidateIndex < positions.length; candidateIndex += 1) {
    if (compareRotations(positions, candidateIndex, smallestIndex) < 0) {
      smallestIndex = candidateIndex;
    }
  }
  return smallestIndex;
}

function compareRotations(positions, leftStart, rightStart) {
  for (let offset = 0; offset < positions.length; offset += 1) {
    const left = positions[(leftStart + offset) % positions.length];
    const right = positions[(rightStart + offset) % positions.length];
    const comparison = comparePositions(left, right);
    if (comparison !== 0) {
      return comparison;
    }
  }
  return 0;
}

function comparePositions(left, right) {
  if (left[0] !== right[0]) {
    return left[0] < right[0] ? -1 : 1;
  }
  if (left[1] !== right[1]) {
    return left[1] < right[1] ? -1 : 1;
  }
  return 0;
}

function compareCoordinateTrees(left, right) {
  return compareStrings(JSON.stringify(left), JSON.stringify(right));
}

function visitPositions(value, visitor) {
  if (
    Array.isArray(value) &&
    value.length === 2 &&
    typeof value[0] === "number" &&
    typeof value[1] === "number"
  ) {
    visitor(value);
    return;
  }
  if (!Array.isArray(value)) {
    throw new TypeError("coordinates содержат значение, которое не является массивом");
  }
  for (const child of value) {
    visitPositions(child, visitor);
  }
}

function signedArea(openRing) {
  let twiceArea = 0;
  for (let index = 0; index < openRing.length; index += 1) {
    const current = openRing[index];
    const next = openRing[(index + 1) % openRing.length];
    twiceArea += current[0] * next[1] - next[0] * current[1];
  }
  return twiceArea / 2;
}

function positionsEqual(left, right) {
  return left[0] === right[0] && left[1] === right[1];
}

function normalizeNegativeZero(value) {
  return Object.is(value, -0) ? 0 : value;
}

function assertCompleteRegionCoverage(seenRegionIds, regionById) {
  if (seenRegionIds.size !== regionById.size) {
    const missing = [...regionById.keys()]
      .filter((regionId) => !seenRegionIds.has(regionId))
      .sort(compareStrings);
    fail("features", `отсутствуют регионы: ${missing.join(", ")}`);
  }
}

function assertUniqueRegionId(seenRegionIds, regionId, path) {
  if (seenRegionIds.has(regionId)) {
    fail(path, `duplicate regionId: ${regionId}`);
  }
  seenRegionIds.add(regionId);
}

function assertStrictObject(value, path, fields) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail(path, "ожидается объект");
  }
  const expected = new Set(fields);
  for (const field of Object.keys(value)) {
    if (!expected.has(field)) {
      fail(`${path}.${field}`, "неожиданное поле");
    }
  }
  for (const field of fields) {
    if (!Object.hasOwn(value, field)) {
      fail(`${path}.${field}`, "обязательное поле отсутствует");
    }
  }
}

function assertNonEmptyArray(value, path) {
  if (!Array.isArray(value) || value.length === 0) {
    fail(path, "ожидается непустой массив");
  }
}

function assertExact(value, expected, path) {
  if (value !== expected) {
    fail(path, `ожидается ${JSON.stringify(expected)}, получено ${JSON.stringify(value)}`);
  }
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function fail(path, message) {
  throw new TypeError(`${path}: ${message}`);
}
