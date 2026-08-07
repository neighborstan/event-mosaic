package com.neighbor.eventmosaic.ingestion.observability;

import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * Собирает диагностическую проекцию PostgreSQL и Elasticsearch без изменения
 * lifecycle state. Короткий cache объединяет серию чтений Micrometer gauges в
 * одну внешнюю проверку.
 */
@Component
final class DefaultBackendDataOperationalState implements BackendDataOperationalState {

	private static final long CACHE_NANOS = Duration.ofSeconds(5).toNanos();

	private final JdbcBackendDataOperationalRepository repository;
	private final IndexMaintenanceGateway indexGateway;
	private final Object refreshMonitor = new Object();

	private volatile CachedSnapshot cachedSnapshot;

	DefaultBackendDataOperationalState(
			JdbcBackendDataOperationalRepository repository,
			IndexMaintenanceGateway indexGateway
	) {
		this.repository = repository;
		this.indexGateway = indexGateway;
	}

	@Override
	public BackendDataOperationalSnapshot observe() {
		synchronized (refreshMonitor) {
			return refresh();
		}
	}

	@Override
	public BackendDataOperationalSnapshot current() {
		long now = System.nanoTime();
		CachedSnapshot current = cachedSnapshot;
		if (current != null && now - current.observedAtNanos() < CACHE_NANOS) {
			return current.snapshot();
		}
		synchronized (refreshMonitor) {
			current = cachedSnapshot;
			if (current != null && now - current.observedAtNanos() < CACHE_NANOS) {
				return current.snapshot();
			}
			return refresh();
		}
	}

	private BackendDataOperationalSnapshot refresh() {
		BackendDataOperationalSnapshot snapshot;
		try {
			BackendDataDatabaseSnapshot database = repository.read();
			AliasObservation aliases = observeAliases(database);
			snapshot = toOperationalSnapshot(aliases.database(), aliases.state());
		}
		catch (RuntimeException _) {
			snapshot = BackendDataOperationalSnapshot.unavailable();
		}
		cachedSnapshot = new CachedSnapshot(snapshot, System.nanoTime());
		return snapshot;
	}

	private AliasObservation observeAliases(BackendDataDatabaseSnapshot database) {
		try {
			IndexMaintenanceGateway.AliasMembership actual = indexGateway.readAliases();
			if (matches(database, actual)) {
				return new AliasObservation(database, AliasConsistencyState.CONSISTENT);
			}
			BackendDataDatabaseSnapshot confirmed = repository.read();
			if (matches(confirmed, actual)) {
				return new AliasObservation(confirmed, AliasConsistencyState.CONSISTENT);
			}
			AliasConsistencyState state = confirmed.aliasTransitionOperations() > 0
					? AliasConsistencyState.MAINTENANCE
					: AliasConsistencyState.INCIDENT;
			return new AliasObservation(confirmed, state);
		}
		catch (RuntimeException _) {
			return new AliasObservation(database, AliasConsistencyState.UNAVAILABLE);
		}
	}

	private static boolean matches(
			BackendDataDatabaseSnapshot database,
			IndexMaintenanceGateway.AliasMembership actual
	) {
		return database.activeEventIndices().equals(actual.eventIndices())
				&& database.activeMentionIndices().equals(actual.mentionIndices());
	}

	private static BackendDataOperationalSnapshot toOperationalSnapshot(
			BackendDataDatabaseSnapshot database,
			AliasConsistencyState aliasState
	) {
		return new BackendDataOperationalSnapshot(
				true,
				database.lagSeconds(),
				database.openGaps(),
				database.sourcePollRetries(),
				database.acquisitionRetries(),
				database.processingRetries(),
				database.permanentFailures(),
				database.receipts(),
				database.generations(),
				database.repairRequiredPartitions(),
				database.openMaintenanceOperations(),
				aliasState);
	}

	private record CachedSnapshot(
			BackendDataOperationalSnapshot snapshot,
			long observedAtNanos
	) {
	}

	private record AliasObservation(
			BackendDataDatabaseSnapshot database,
			AliasConsistencyState state
	) {
	}
}
