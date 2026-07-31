package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Преобразует raw Event 61 в целевую immutable read model.
 */
@Component
final class GdeltEventDocumentMapper {

	private final EventGeoSelector geoSelector;

	GdeltEventDocumentMapper() {
		this.geoSelector = new EventGeoSelector();
	}

	DocumentMappingResult<IndexedEventDocument> map(
			GdeltCsvRecord<GdeltEvent> sourceRecord,
			ArchiveProcessingRequest request
	) {
		Objects.requireNonNull(sourceRecord, "sourceRecord must not be null");
		Objects.requireNonNull(request, "request must not be null");
		GdeltEvent event = sourceRecord.value();
		if (event.globalEventId() <= 0) {
			return DocumentMappingResult.rejected(ProcessingMappingRejection.EVENT_ID_INVALID);
		}
		if (event.day() == null) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.EVENT_DAY_MISSING);
		}
		if (event.dateAdded() == null) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.EVENT_DATE_ADDED_MISSING);
		}

		EventGeoSelector.Selection geo = geoSelector.select(event);
		return DocumentMappingResult.accepted(new IndexedEventDocument(
				event.globalEventId(),
				event.day(),
				event.dateAdded(),
				event.actor1Code(),
				event.actor1Name(),
				event.actor1CountryCode(),
				event.actor1KnownGroupCode(),
				event.actor1EthnicCode(),
				event.actor1Religion1Code(),
				event.actor1Religion2Code(),
				event.actor1Type1Code(),
				event.actor1Type2Code(),
				event.actor1Type3Code(),
				event.actor2Code(),
				event.actor2Name(),
				event.actor2CountryCode(),
				event.actor2KnownGroupCode(),
				event.actor2EthnicCode(),
				event.actor2Religion1Code(),
				event.actor2Religion2Code(),
				event.actor2Type1Code(),
				event.actor2Type2Code(),
				event.actor2Type3Code(),
				event.isRootEvent(),
				event.eventCode(),
				event.eventBaseCode(),
				event.eventRootCode(),
				event.quadClass(),
				event.goldsteinScale(),
				event.avgTone(),
				event.numMentions(),
				event.numSources(),
				event.numArticles(),
				geo.location(),
				event.sourceUrl(),
				request.sourceUpdateTime(),
				request.sourceArchiveKey(),
				sourceRecord.lineNumber(),
				request.processingFingerprint()), geo.invalidCandidateCount());
	}
}
