package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvProgressListener;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.gdelt.api.GdeltMentionCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
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

	@Override
	public GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltMention> consumer,
			GdeltCsvProgressListener progressListener
	) {
		return readCsv(csvPath, consumer, progressListener);
	}

	@Override
	public GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltMention> consumer,
			OperationBudget budget
	) {
		return readCsv(csvPath, consumer, budget);
	}

	@Override
	public GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltMention> consumer,
			GdeltCsvProgressListener progressListener,
			OperationBudget budget
	) {
		return readCsv(csvPath, consumer, progressListener, budget);
	}
}
