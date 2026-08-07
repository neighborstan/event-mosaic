package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Operational snapshot backend data из PostgreSQL")
class JdbcBackendDataOperationalRepositoryIntegrationTest {

	@Autowired
	private JdbcBackendDataOperationalRepository repository;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanState() {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    ingestion_source_poll_state,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs,
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Первый известный update без indexed pair дает один полный интервал lag")
	void includesFirstKnownUpdateInLag() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				) values (
				    :sourceName,
				    :updateTime,
				    :updateTime,
				    'LATEST',
				    :updateTime,
				    :updateTime
				)
				""")
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("updateTime", Timestamp.from(updateTime))
				.update();

		BackendDataDatabaseSnapshot snapshot = repository.read();

		assertThat(snapshot.lagSeconds()).isEqualTo(900);
	}
}
