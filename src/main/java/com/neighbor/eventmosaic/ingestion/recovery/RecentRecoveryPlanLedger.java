package com.neighbor.eventmosaic.ingestion.recovery;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Хранит план восстановления данных за последние сутки и связанные с ним архивы.
 * Сохраненное состояние позволяет после перезапуска продолжить точно с оставшихся пропусков,
 * не скачивая уже известные архивы повторно.
 */
public interface RecentRecoveryPlanLedger {

	/**
	 * Одной транзакцией сохраняет последнюю пару архивов, границы окна и обнаруженные пропуски.
	 */
	ActivationResult activate(DiscoveredUpdate newestUpdate, RecentWindowPlan plan);

	/** Возвращает текущий сохраненный план, если восстановление последнего окна уже начато. */
	Optional<PlanState> currentPlan();

	/**
	 * Сохраняет архивы из проверенной версии полного каталога GDELT. Повтор той же операции не дублирует данные.
	 */
	CatalogRegistrationResult registerCatalog(CatalogRegistration registration);

	/** Возвращает интервалы, для которых в текущем плане еще нет полной пары архивов Event и Mention. */
	List<Gap> remainingGaps();

	/** Возвращает сохраненное число обнаруженных интервалов и пропущенных обновлений. */
	GapAuditSummary gapAuditSummary();

	/** Возвращает архивы указанного вида из текущего плана, начиная с самого нового. */
	List<IngestionArchiveState> archivesNewestFirst(ArchiveType archiveType);

	/** Показывает, как запрос на активацию изменил план или был отклонен из-за более новых данных. */
	enum ActivationOutcome {
		ACTIVATED,
		REVISED,
		UNCHANGED,
		STALE_FRONTIER
	}

	/**
	 * Возвращает итог активации, актуальный план и число впервые сохраненных интервалов с пропусками.
	 *
	 * @param outcome итог применения плана
	 * @param plan актуальный план после операции, если он существует
	 * @param auditGapsCreated число новых интервалов с пропусками
	 */
	record ActivationResult(
			ActivationOutcome outcome,
			Optional<PlanState> plan,
			int auditGapsCreated
	) {
		/** Проверяет обязательные поля и неотрицательное число новых пропусков. */
		public ActivationResult {
			Objects.requireNonNull(outcome, "outcome must not be null");
			plan = Objects.requireNonNull(plan, "plan must not be null");
			if (auditGapsCreated < 0) {
				throw new IllegalArgumentException("auditGapsCreated must not be negative");
			}
		}
	}

	/** Показывает, подтверждено ли полным каталогом GDELT все временное окно текущей версии плана. */
	enum CatalogStatus {
		PENDING,
		CATALOG_COMPLETE
	}

	/**
	 * Однозначно описывает версию плана. Номер версии растет, когда меняются границы окна или самое новое обновление.
	 *
	 * @param generation порядковый номер версии плана
	 * @param windowFrom включаемое начало суточного окна
	 * @param windowTo исключаемый конец суточного окна
	 * @param sourceFrontier самое новое обновление, известное в момент создания плана
	 */
	record Revision(
			long generation,
			Instant windowFrom,
			Instant windowTo,
			Instant sourceFrontier
	) {
		/** Проверяет положительный номер версии и согласованные границы окна. */
		public Revision {
			if (generation <= 0) {
				throw new IllegalArgumentException("generation must be positive");
			}
			RecentWindowPlan.fromBoundaries(windowFrom, windowTo, sourceFrontier);
		}
	}

	/**
	 * Хранит текущее состояние плана и сведения о версии полного каталога GDELT, по которой был проверен этот план.
	 * Сведения о каталоге либо заполнены полностью, либо отсутствуют.
	 */
	record PlanState(
			Revision revision,
			CatalogStatus catalogStatus,
			String masterGeneration,
			String masterEtag,
			Instant masterVerifiedFrom,
			Instant masterVerifiedAt,
			long stateVersion
	) {
		/** Проверяет, что сведения о полном каталоге заполнены целиком или полностью отсутствуют. */
		public PlanState {
			Objects.requireNonNull(revision, "revision must not be null");
			Objects.requireNonNull(catalogStatus, "catalogStatus must not be null");
			boolean empty = masterGeneration == null && masterEtag == null
					&& masterVerifiedFrom == null && masterVerifiedAt == null;
			boolean complete = masterGeneration != null && masterEtag != null
					&& masterVerifiedFrom != null && masterVerifiedAt != null;
			if (!empty && !complete) {
				throw new IllegalArgumentException("master evidence must be complete or absent");
			}
			if (stateVersion < 0) {
				throw new IllegalArgumentException("stateVersion must not be negative");
			}
		}
	}

	/**
	 * Передает для сохранения архивы, найденные в одной закрепленной версии полного каталога GDELT.
	 * Номер версии плана не дает применить эти данные к уже обновленному плану.
	 */
	record CatalogRegistration(
			Revision revision,
			List<DiscoveredUpdate> updates,
			String masterGeneration,
			String masterEtag,
			Instant masterVerifiedFrom,
			boolean catalogComplete
	) {
		/** Копирует список обновлений и проверяет обязательные сведения о версии полного каталога. */
		public CatalogRegistration {
			Objects.requireNonNull(revision, "revision must not be null");
			updates = List.copyOf(updates);
			if (masterGeneration == null || !masterGeneration.matches("[0-9]+")) {
				throw new IllegalArgumentException("masterGeneration must contain digits");
			}
			if (masterEtag == null || masterEtag.isBlank()) {
				throw new IllegalArgumentException("masterEtag must not be blank");
			}
			Objects.requireNonNull(masterVerifiedFrom, "masterVerifiedFrom must not be null");
			if (catalogComplete && masterVerifiedFrom.isAfter(revision.windowFrom())) {
				throw new IllegalArgumentException("complete catalog must prove the lower boundary");
			}
		}
	}

	/** Показывает, были ли данные каталога сохранены, уже были известны или относились к устаревшему плану. */
	enum CatalogRegistrationOutcome {
		APPLIED,
		ALREADY_COMPLETE,
		STALE_PLAN
	}

	/**
	 * Возвращает итог сохранения данных полного каталога и состояние плана после операции.
	 *
	 * @param outcome итог сохранения
	 * @param plan актуальное состояние плана
	 */
	record CatalogRegistrationResult(
			CatalogRegistrationOutcome outcome,
			PlanState plan
	) {
		/** Проверяет наличие итога и актуального состояния плана. */
		public CatalogRegistrationResult {
			Objects.requireNonNull(outcome, "outcome must not be null");
			Objects.requireNonNull(plan, "plan must not be null");
		}
	}

	/**
	 * Описывает непрерывный интервал без полных пар архивов Event и Mention. Обе границы входят в интервал.
	 *
	 * @param firstMissingUpdateTime время первого отсутствующего обновления
	 * @param lastMissingUpdateTime время последнего отсутствующего обновления
	 */
	record Gap(Instant firstMissingUpdateTime, Instant lastMissingUpdateTime) {
		/** Проверяет возрастающий непустой интервал. */
		public Gap {
			Objects.requireNonNull(firstMissingUpdateTime, "firstMissingUpdateTime must not be null");
			Objects.requireNonNull(lastMissingUpdateTime, "lastMissingUpdateTime must not be null");
			if (firstMissingUpdateTime.isAfter(lastMissingUpdateTime)) {
				throw new IllegalArgumentException("gap boundaries are reversed");
			}
		}
	}

	/**
	 * Хранит общее число сохраненных интервалов с пропусками и обновлений внутри них.
	 *
	 * @param intervalCount число отдельных интервалов
	 * @param slotCount общее число пропущенных 15-минутных обновлений
	 */
	record GapAuditSummary(long intervalCount, long slotCount) {
		/** Проверяет, что оба счетчика неотрицательны. */
		public GapAuditSummary {
			if (intervalCount < 0 || slotCount < 0) {
				throw new IllegalArgumentException("gap audit counts must not be negative");
			}
		}
	}
}
