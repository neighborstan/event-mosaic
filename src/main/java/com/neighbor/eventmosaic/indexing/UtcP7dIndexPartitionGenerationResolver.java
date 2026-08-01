package com.neighbor.eventmosaic.indexing;

import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolution;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolver;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

/** ISO Monday-UTC implementation утвержденного P7D partitioning. */
@Component
final class UtcP7dIndexPartitionGenerationResolver
		implements IndexPartitionGenerationResolver {

	private static final Period INTERVAL = Period.ofDays(7);
	private static final String PARTITION_PREFIX = "p";

	@Override
	public IndexPartitionGenerationResolution resolve(
			Instant sourceUpdateTime,
			int generationNumber
	) {
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		if (generationNumber <= 0) {
			throw new IllegalArgumentException("generationNumber must be positive");
		}
		LocalDate startDate = LocalDate.ofInstant(sourceUpdateTime, ZoneOffset.UTC)
				.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
		Instant startAt = startDate.atStartOfDay(ZoneOffset.UTC).toInstant();
		String partitionKey = PARTITION_PREFIX
				+ DateTimeFormatter.BASIC_ISO_DATE.format(startDate);
		String generationSuffix = String.format(Locale.ROOT, "%04d", generationNumber);
		IndexPartitionDefinition partition = new IndexPartitionDefinition(
				partitionKey,
				startAt,
				startDate.plus(INTERVAL).atStartOfDay(ZoneOffset.UTC).toInstant(),
				INTERVAL);
		return new IndexPartitionGenerationResolution(
				partition,
				new IndexGenerationNames(
						"gdelt-events-v1-" + partitionKey + "-g" + generationSuffix,
						"gdelt-mentions-v1-" + partitionKey + "-g" + generationSuffix));
	}
}
