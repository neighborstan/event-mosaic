package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageInterval;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageUnavailableException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Проверяет суточную полноту Event ingestion одним read-only запросом к V001
 * ledger и не зависит от состояния Mention archive.
 */
@Repository
public class JdbcIngestionCoverageQuery implements IngestionCoverageQuery {

	private static final Duration WINDOW_DURATION = Duration.ofHours(24);
	private static final Duration SLOT_DURATION = GdeltSourceContract.UPDATE_INTERVAL;
	private static final int EXPECTED_SLOT_COUNT = 96;
	private static final String PARAM_SOURCE_NAME = "sourceName";

	private static final String COVERAGE_SQL = """
			with source_state as (
			    select source_name, continuity_baseline, latest_observed_update_time
			    from ingestion_source_state
			    where source_name = :sourceName
			),
			slots as (
			    select series.source_update_time
			    from generate_series(
			        cast(:windowFrom as timestamptz),
			        cast(:windowTo as timestamptz) - (:slotSeconds * interval '1 second'),
			        :slotSeconds * interval '1 second'
			    ) as series(source_update_time)
			)
			select
			    source_state.source_name is not null as initialized,
			    slots.source_update_time,
			    coalesce(
			        slots.source_update_time >= source_state.continuity_baseline
			        and slots.source_update_time <= source_state.latest_observed_update_time
			        and exists (
			            select 1
			            from ingestion_runs ingestion_run
			            join ingestion_archives archive
			              on archive.run_id = ingestion_run.id
			             and archive.source_update_time = ingestion_run.source_update_time
			            join ingestion_archive_processing processing
			              on processing.archive_idempotency_key = archive.idempotency_key
			            join index_generations active_generation
			              on active_generation.id = processing.verified_generation_id
			             and active_generation.partition_key = processing.logical_partition_key
			             and active_generation.state = 'ACTIVE'
			            join index_logical_partitions logical_partition
			              on logical_partition.partition_key = active_generation.partition_key
			            where ingestion_run.source_name = :sourceName
			              and ingestion_run.source_update_time = slots.source_update_time
			              and archive.archive_type = 'TRANSLATION_EVENTS'
			              and archive.status = 'STAGED'
			              and processing.status = 'INDEXED'
			              and processing.bound_index_kind = 'EVENT'
			              and processing.bound_generation_id = active_generation.id
			              and processing.bound_generation_uuid = active_generation.generation_uuid
			              and processing.bound_index_name = active_generation.event_index_name
			              and processing.bound_index_uuid = active_generation.event_index_uuid
			              and processing.verified_index_uuid = active_generation.event_index_uuid
			              and processing.receipt_digest_algorithm = 'sha256-length-prefix-v1'
			              and processing.expected_document_count = processing.actual_document_count
			              and processing.expected_identity_digest = processing.actual_identity_digest
			              and slots.source_update_time >= logical_partition.partition_start_at
			              and slots.source_update_time < logical_partition.partition_end_at
			        ),
			        false
			    ) as covered
			from slots
			left join source_state on true
			order by slots.source_update_time
			""";

	private final JdbcClient jdbcClient;

	/**
	 * Создает запрос поверх общего JDBC client приложения.
	 *
	 * @param jdbcClient JDBC client для ingestion ledger
	 */
	public JdbcIngestionCoverageQuery(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	@Override
	public IngestionCoverageEvidence read(Instant from, Instant to) {
		validateWindow(from, to);
		List<SlotCoverage> slots;
		try {
			slots = jdbcClient.sql(COVERAGE_SQL)
					.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
					.param("windowFrom", Timestamp.from(from))
					.param("windowTo", Timestamp.from(to))
					.param("slotSeconds", SLOT_DURATION.toSeconds())
					.query((resultSet, rowNumber) -> new SlotCoverage(
							resultSet.getBoolean("initialized"),
							IngestionJdbcMappers.instant(resultSet, "source_update_time"),
							resultSet.getBoolean("covered")))
					.list();
		} catch (DataAccessException exception) {
			throw new IngestionCoverageUnavailableException(exception);
		}

		validateGrid(slots, from);
		if (!slots.get(0).initialized()) {
			return IngestionCoverageEvidence.unknown();
		}
		List<IngestionCoverageInterval> missingIntervals = missingIntervals(slots, from, to);
		return missingIntervals.isEmpty()
				? IngestionCoverageEvidence.complete()
				: IngestionCoverageEvidence.partial(missingIntervals);
	}

	private static void validateWindow(Instant from, Instant to) {
		Objects.requireNonNull(from, "from must not be null");
		Objects.requireNonNull(to, "to must not be null");
		if (!GdeltSourceContract.isUpdateBoundary(from)
				|| !GdeltSourceContract.isUpdateBoundary(to)) {
			throw new IllegalArgumentException("Coverage window must align to 15-minute UTC boundaries");
		}
		if (!WINDOW_DURATION.equals(Duration.between(from, to))) {
			throw new IllegalArgumentException("Coverage window must be exactly 24 hours");
		}
	}

	private static void validateGrid(List<SlotCoverage> slots, Instant from) {
		if (slots.size() != EXPECTED_SLOT_COUNT) {
			throw new IllegalStateException("Coverage query returned an unexpected slot count");
		}
		boolean initialized = slots.get(0).initialized();
		for (int index = 0; index < slots.size(); index++) {
			SlotCoverage slot = slots.get(index);
			Instant expectedUpdateTime = from.plus(SLOT_DURATION.multipliedBy(index));
			if (!expectedUpdateTime.equals(slot.sourceUpdateTime())) {
				throw new IllegalStateException("Coverage query returned an invalid slot grid");
			}
			if (slot.initialized() != initialized) {
				throw new IllegalStateException("Coverage query returned inconsistent source state");
			}
			if (!initialized && slot.covered()) {
				throw new IllegalStateException("Uninitialized source state must not contain covered slots");
			}
		}
	}

	private static List<IngestionCoverageInterval> missingIntervals(
			List<SlotCoverage> slots,
			Instant windowFrom,
			Instant windowTo
	) {
		List<IngestionCoverageInterval> intervals = new ArrayList<>();
		Instant missingFrom = null;
		Instant missingTo = null;
		for (SlotCoverage slot : slots) {
			if (slot.covered()) {
				if (missingFrom != null) {
					intervals.add(new IngestionCoverageInterval(missingFrom, missingTo));
					missingFrom = null;
					missingTo = null;
				}
				continue;
			}

			Instant slotFrom = laterOf(slot.sourceUpdateTime(), windowFrom);
			Instant slotTo = earlierOf(slot.sourceUpdateTime().plus(SLOT_DURATION), windowTo);
			if (!slotFrom.isBefore(slotTo)) {
				continue;
			}
			if (missingFrom == null) {
				missingFrom = slotFrom;
			}
			missingTo = slotTo;
		}
		if (missingFrom != null) {
			intervals.add(new IngestionCoverageInterval(missingFrom, missingTo));
		}
		return List.copyOf(intervals);
	}

	private static Instant laterOf(Instant left, Instant right) {
		return left.isAfter(right) ? left : right;
	}

	private static Instant earlierOf(Instant left, Instant right) {
		return left.isBefore(right) ? left : right;
	}

	private record SlotCoverage(boolean initialized, Instant sourceUpdateTime, boolean covered) {
	}
}
