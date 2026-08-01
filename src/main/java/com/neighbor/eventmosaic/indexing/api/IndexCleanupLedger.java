package com.neighbor.eventmosaic.indexing.api;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Хранит fenced cleanup plan и выполняет его переходы под partition lock.
 * Физическую проверку aliases и удаление индексов выполняет service layer.
 */
public interface IndexCleanupLedger {

	/** Возвращает только inspect candidates, подходящие по состоянию и возрасту. */
	List<CleanupCandidateSnapshot> findCandidates(
			String partitionKey,
			Duration orphanBuildingAge,
			Duration supersededAge);

	/**
	 * Захватывает неизменившийся candidate и переводит generation в
	 * CLEANUP_PENDING. Пустой результат означает, что snapshot уже устарел.
	 */
	Optional<CleanupOperation> claim(CleanupClaim claim, Duration leaseDuration);

	/**
	 * Возобновляет cleanup с тем же token. Новый token для уже принятого плана
	 * не создается, в том числе после DELETE_REQUESTED.
	 */
	Optional<CleanupOperation> resume(
			String partitionKey,
			String planFingerprint,
			Duration leaseDuration);

	/** Продлевает lease текущего владельца с полным CAS и тем же token. */
	Optional<CleanupOperation> renew(
			CleanupOwnership ownership,
			Duration leaseDuration);

	/**
	 * Записывает DELETE_REQUESTED после свежей внешней проверки aliases и UUID.
	 */
	CleanupTransitionResult requestDelete(
			CleanupOwnership ownership,
			CleanupDeleteEvidence evidence);

	/**
	 * Записывает CLEANED только после подтверждения отсутствия обоих точных UUID.
	 */
	CleanupTransitionResult complete(
			CleanupOwnership ownership,
			CleanupCompletionEvidence evidence);

	/** Возвращает незавершенную cleanup operation без смены ее token. */
	Optional<CleanupOperation> findRecoverable(String partitionKey);

	/** Проверяет общий maintenance barrier, включая rebuild и cleanup. */
	boolean hasOpenMaintenance(String partitionKey);
}
