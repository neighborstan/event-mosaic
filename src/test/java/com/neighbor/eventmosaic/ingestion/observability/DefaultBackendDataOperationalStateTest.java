package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Сверка operational state с stable aliases")
class DefaultBackendDataOperationalStateTest {

	private static final String EVENT_INDEX = "gdelt-events-v1-p20260727-g0001";
	private static final String MENTION_INDEX = "gdelt-mentions-v1-p20260727-g0001";

	private final JdbcBackendDataOperationalRepository repository =
			mock(JdbcBackendDataOperationalRepository.class);
	private final IndexMaintenanceGateway indexGateway = mock(IndexMaintenanceGateway.class);
	private final DefaultBackendDataOperationalState state =
			new DefaultBackendDataOperationalState(repository, indexGateway);

	@Test
	@DisplayName("Совпавшие ACTIVE generations и aliases дают consistent state")
	void classifiesMatchingAliasesAsConsistent() {
		when(repository.read()).thenReturn(databaseSnapshot(0, 0));
		when(indexGateway.readAliases()).thenReturn(new IndexMaintenanceGateway.AliasMembership(
				Set.of(EVENT_INDEX),
				Set.of(MENTION_INDEX)));

		BackendDataOperationalSnapshot snapshot = state.observe();

		assertThat(snapshot.databaseAvailable()).isTrue();
		assertThat(snapshot.aliasConsistency()).isEqualTo(AliasConsistencyState.CONSISTENT);
	}

	@Test
	@DisplayName("Расхождение aliases вне maintenance дает bounded incident")
	void classifiesUnexpectedAliasesAsIncident() {
		when(repository.read()).thenReturn(databaseSnapshot(0, 0));
		when(indexGateway.readAliases()).thenReturn(new IndexMaintenanceGateway.AliasMembership(
				Set.of(),
				Set.of()));

		assertThat(state.observe().aliasConsistency())
				.isEqualTo(AliasConsistencyState.INCIDENT);
	}

	@Test
	@DisplayName("Расхождение aliases во время lifecycle operation остается maintenance state")
	void classifiesTransitionalAliasesAsMaintenance() {
		when(repository.read()).thenReturn(databaseSnapshot(1, 1));
		when(indexGateway.readAliases()).thenReturn(new IndexMaintenanceGateway.AliasMembership(
				Set.of(),
				Set.of()));

		assertThat(state.observe().aliasConsistency())
				.isEqualTo(AliasConsistencyState.MAINTENANCE);
	}

	@Test
	@DisplayName("Cleanup operation не маскирует независимое расхождение aliases")
	void cleanupDoesNotMaskAliasIncident() {
		when(repository.read()).thenReturn(databaseSnapshot(1, 0));
		when(indexGateway.readAliases()).thenReturn(new IndexMaintenanceGateway.AliasMembership(
				Set.of(),
				Set.of()));

		assertThat(state.observe().aliasConsistency())
				.isEqualTo(AliasConsistencyState.INCIDENT);
	}

	@Test
	@DisplayName("Повторный PostgreSQL snapshot устраняет ложный incident при cutover")
	void confirmsMismatchAgainstOneBoundedDatabaseReread() {
		BackendDataDatabaseSnapshot beforeCutover = databaseSnapshot(1, 1);
		BackendDataDatabaseSnapshot afterCutover = databaseSnapshot(
				0,
				0,
				Set.of(),
				Set.of());
		when(repository.read()).thenReturn(beforeCutover, afterCutover);
		when(indexGateway.readAliases()).thenReturn(new IndexMaintenanceGateway.AliasMembership(
				Set.of(),
				Set.of()));

		assertThat(state.observe().aliasConsistency())
				.isEqualTo(AliasConsistencyState.CONSISTENT);
		verify(repository, times(2)).read();
	}

	@Test
	@DisplayName("Недоступная alias проверка не скрывается как consistent")
	void reportsUnavailableAliasObservation() {
		when(repository.read()).thenReturn(databaseSnapshot(0, 0));
		when(indexGateway.readAliases()).thenThrow(new IllegalStateException("external failure"));

		BackendDataOperationalSnapshot snapshot = state.observe();

		assertThat(snapshot.databaseAvailable()).isTrue();
		assertThat(snapshot.aliasConsistency()).isEqualTo(AliasConsistencyState.UNAVAILABLE);
	}

	@Test
	@DisplayName("Недоступный PostgreSQL snapshot возвращает bounded unavailable state")
	void reportsUnavailableDatabaseObservation() {
		when(repository.read()).thenThrow(new IllegalStateException("database failure"));

		BackendDataOperationalSnapshot snapshot = state.observe();

		assertThat(snapshot.databaseAvailable()).isFalse();
		assertThat(snapshot.aliasConsistency()).isEqualTo(AliasConsistencyState.UNAVAILABLE);
	}

	private static BackendDataDatabaseSnapshot databaseSnapshot(
			long openMaintenance,
			long aliasTransitions
	) {
		return databaseSnapshot(
				openMaintenance,
				aliasTransitions,
				Set.of(EVENT_INDEX),
				Set.of(MENTION_INDEX));
	}

	private static BackendDataDatabaseSnapshot databaseSnapshot(
			long openMaintenance,
			long aliasTransitions,
			Set<String> eventIndices,
			Set<String> mentionIndices
	) {
		return new BackendDataDatabaseSnapshot(
				0,
				0,
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				0,
				new BackendDataOperationalSnapshot.ReceiptCounts(0, 0),
				new BackendDataOperationalSnapshot.GenerationCounts(0, 0, 0),
				0,
				openMaintenance,
				aliasTransitions,
				eventIndices,
				mentionIndices);
	}
}
