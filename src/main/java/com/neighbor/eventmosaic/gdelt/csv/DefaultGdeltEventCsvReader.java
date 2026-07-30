package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltEventCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/**
 * Production reader GDELT Translation Event CSV из 61 поля.
 */
@Component
final class DefaultGdeltEventCsvReader extends GdeltCsvReaderSupport<GdeltEvent>
		implements GdeltEventCsvReader {

	DefaultGdeltEventCsvReader(GdeltCsvProperties properties, GdeltCsvMetrics metrics) {
		super(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				GdeltEventCsvMapper.FIELD_COUNT,
				properties,
				new GdeltEventCsvMapper(),
				metrics);
	}

	@Override
	public GdeltCsvReadSummary read(Path csvPath, GdeltRecordConsumer<GdeltEvent> consumer) {
		return readCsv(csvPath, consumer);
	}
}
