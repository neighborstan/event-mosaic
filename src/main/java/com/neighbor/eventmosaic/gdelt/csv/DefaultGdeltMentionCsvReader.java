package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.gdelt.api.GdeltMentionCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/**
 * Production reader GDELT Translation Mention CSV из 16 полей.
 */
@Component
final class DefaultGdeltMentionCsvReader extends GdeltCsvReaderSupport<GdeltMention>
		implements GdeltMentionCsvReader {

	DefaultGdeltMentionCsvReader(GdeltCsvProperties properties, GdeltCsvMetrics metrics) {
		super(
				GdeltArchiveKind.TRANSLATION_MENTIONS,
				GdeltMentionCsvMapper.FIELD_COUNT,
				properties,
				new GdeltMentionCsvMapper(),
				metrics);
	}

	@Override
	public GdeltCsvReadSummary read(Path csvPath, GdeltRecordConsumer<GdeltMention> consumer) {
		return readCsv(csvPath, consumer);
	}
}
