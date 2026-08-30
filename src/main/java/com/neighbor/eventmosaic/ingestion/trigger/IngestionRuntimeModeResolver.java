package com.neighbor.eventmosaic.ingestion.trigger;

/**
 * Выбирает единственный режим процесса до создания механизмов запуска.
 * Явный однократный запуск и служебные команды имеют приоритет над
 * автоматическим режимом, а несовместимые команды отклоняются до внешнего
 * ввода-вывода.
 */
public final class IngestionRuntimeModeResolver {

	/**
	 * Выбирает режим по типу процесса и явно запрошенным командам.
	 *
	 * @param ordinaryWebRuntime обычный процесс с HTTP-сервером
	 * @param automaticEnabled разрешена ли автоматическая загрузка для веб-процесса
	 * @param oneShotEnabled запрошен ли один цикл загрузки
	 * @param partitionRebuildRequested запрошена ли команда восстановления partition
	 * @param generationCleanupRequested запрошена ли команда очистки generation
	 * @return единственный режим запуска для текущего процесса
	 * @throws IllegalArgumentException если одновременно запрошены несовместимые команды
	 */
	public IngestionRuntimeMode resolve(
			boolean ordinaryWebRuntime,
			boolean automaticEnabled,
			boolean oneShotEnabled,
			boolean partitionRebuildRequested,
			boolean generationCleanupRequested
	) {
		if (partitionRebuildRequested && generationCleanupRequested) {
			throw new IllegalArgumentException(
					"Only one maintenance command can run in one process");
		}

		boolean maintenanceRequested = partitionRebuildRequested || generationCleanupRequested;
		if (oneShotEnabled && maintenanceRequested) {
			throw new IllegalArgumentException(
					"Ingestion one-shot cannot run with a maintenance command");
		}
		if (maintenanceRequested) {
			return IngestionRuntimeMode.MAINTENANCE;
		}
		if (oneShotEnabled) {
			return IngestionRuntimeMode.ONE_SHOT;
		}
		if (ordinaryWebRuntime && automaticEnabled) {
			return IngestionRuntimeMode.AUTOMATIC;
		}
		return IngestionRuntimeMode.DISABLED;
	}
}
