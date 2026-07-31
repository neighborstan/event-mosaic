package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Преобразует raw Mention 16 в отдельную observation read model.
 */
@Component
final class GdeltMentionDocumentMapper {

	private final MentionIdentityFactory identityFactory;

	GdeltMentionDocumentMapper() {
		this.identityFactory = new MentionIdentityFactory();
	}

	DocumentMappingResult<IndexedMentionDocument> map(
			GdeltCsvRecord<GdeltMention> sourceRecord,
			ArchiveProcessingRequest request
	) {
		Objects.requireNonNull(sourceRecord, "sourceRecord must not be null");
		Objects.requireNonNull(request, "request must not be null");
		GdeltMention mention = sourceRecord.value();
		if (mention.globalEventId() <= 0) {
			return DocumentMappingResult.rejected(ProcessingMappingRejection.MENTION_ID_INVALID);
		}
		if (mention.eventTimeDate() == null) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.MENTION_EVENT_TIME_MISSING);
		}
		if (mention.mentionTimeDate() == null) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.MENTION_TIME_MISSING);
		}
		if (mention.mentionType() == null) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.MENTION_TYPE_MISSING);
		}
		if (mention.mentionIdentifier() == null || mention.mentionIdentifier().isBlank()) {
			return DocumentMappingResult.rejected(
					ProcessingMappingRejection.MENTION_IDENTIFIER_MISSING);
		}

		String rawMentionId = identityFactory.rawMentionId(
				request.sourceArchiveKey(),
				sourceRecord.lineNumber());
		String sourceDocumentKey = identityFactory.sourceDocumentKey(
				mention.globalEventId(),
				mention.mentionType(),
				mention.mentionIdentifier());
		return DocumentMappingResult.accepted(new IndexedMentionDocument(
				rawMentionId,
				sourceDocumentKey,
				mention.globalEventId(),
				mention.eventTimeDate(),
				mention.mentionTimeDate(),
				mention.mentionType(),
				mention.mentionSourceName(),
				mention.mentionIdentifier(),
				mention.sentenceId(),
				mention.actor1CharOffset(),
				mention.actor2CharOffset(),
				mention.actionCharOffset(),
				mention.inRawText(),
				mention.confidence(),
				mention.mentionDocLen(),
				mention.mentionDocTone(),
				mention.mentionDocTranslationInfo(),
				mention.extras(),
				request.sourceUpdateTime(),
				request.sourceArchiveKey(),
				sourceRecord.lineNumber(),
				request.processingFingerprint()), 0);
	}
}
