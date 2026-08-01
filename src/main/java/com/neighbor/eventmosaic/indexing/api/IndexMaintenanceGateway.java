package com.neighbor.eventmosaic.indexing.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Выполняет только exact-name операции Elasticsearch для подтверждаемого
 * rebuild и явной очистки одной logical partition.
 */
public interface IndexMaintenanceGateway {

	/** Идемпотентно устанавливает актуальные строгие templates. */
	void installTemplates();

	/** Возвращает фактическую identity и write-block exact index. */
	Optional<ObservedIndex> observeExactIndex(String indexName);

	/**
	 * Возвращает фактическую identity и полный набор aliases exact index. В
	 * отличие от {@link #readAliases()} результат не ограничен двумя стабильными
	 * read aliases и позволяет безопасно остановить destructive cleanup при
	 * любом дополнительном membership.
	 */
	Optional<ExactIndexAliasMembership> observeAllAliasesForExactIndex(String indexName);

	/** Возвращает фактический total store size exact UUID без alias или wildcard. */
	long exactIndexStoreBytes(ExactIndexTarget target);

	/** Создает exact physical index либо подтверждает уже существующий. */
	void createExactIndex(String indexName);

	/** Возвращает полное membership двух стабильных read aliases. */
	AliasMembership readAliases();

	/** Идемпотентно добавляет отсутствующие exact alias memberships. */
	void addStableAliases(
			String eventIndexName,
			boolean addEvent,
			String mentionIndexName,
			boolean addMention
	);

	/**
	 * Наблюдаемо устанавливает dedicated write block на существующие exact
	 * indices. Пустой список означает доказанное отсутствие обоих targets.
	 */
	void addWriteBlock(List<ExactIndexTarget> targets);

	/**
	 * Идемпотентно снимает dedicated write block с сохранившихся exact indices и
	 * подтверждает фактическое состояние.
	 */
	void removeWriteBlock(List<ExactIndexTarget> targets);

	/** Выполняет один exact remove/add aliases request. */
	void cutoverAliases(AliasCutover cutover);

	/**
	 * Удаляет один exact physical index после повторной проверки UUID и
	 * отсутствия любых aliases. Отсутствующий target и несовпавший UUID
	 * отклоняются, чтобы вызывающий cleanup protocol сначала согласовал
	 * фактическое состояние.
	 */
	void deleteExactIndex(ExactIndexTarget target);

	/** Возвращает минимально доступное место среди Elasticsearch data nodes. */
	long minimumAvailableDiskBytes();

	/** Делает подтвержденные записи exact index видимыми для receipt query. */
	void refreshExact(GdeltIndexKind kind, ExactIndexTarget target);

	/**
	 * Проверяет aggregate receipt exact generation, в том числе write-blocked
	 * rollback copy.
	 */
	ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query);

	/** Фактическое состояние одного exact physical index. */
	record ObservedIndex(String indexName, String indexUuid, boolean writeBlocked) {

		/** Проверяет bounded exact identity без wildcard. */
		public ObservedIndex {
			requireExact(indexName, "indexName");
			requireExact(indexUuid, "indexUuid");
		}
	}

	/** Фактическая identity exact index и все связанные с ним aliases. */
	record ExactIndexAliasMembership(
			String indexName,
			String indexUuid,
			Set<String> aliases
	) {

		/** Проверяет exact identity и сохраняет immutable alias set. */
		public ExactIndexAliasMembership {
			requireExact(indexName, "indexName");
			requireExact(indexUuid, "indexUuid");
			aliases = Set.copyOf(Objects.requireNonNull(aliases, "aliases must not be null"));
			aliases.forEach(alias -> requireExact(alias, "alias"));
		}
	}

	/** Полное membership глобальных Event/Mention read aliases. */
	record AliasMembership(Set<String> eventIndices, Set<String> mentionIndices) {

		/** Создает защищенные immutable множества exact index names. */
		public AliasMembership {
			eventIndices = Set.copyOf(Objects.requireNonNull(
					eventIndices, "eventIndices must not be null"));
			mentionIndices = Set.copyOf(Objects.requireNonNull(
					mentionIndices, "mentionIndices must not be null"));
			eventIndices.forEach(name -> requireExact(name, "event alias member"));
			mentionIndices.forEach(name -> requireExact(name, "mention alias member"));
		}
	}

	/**
	 * Точный conditional switch. Старые targets nullable только для repair
	 * отсутствующей current generation.
	 */
	record AliasCutover(
			ExactIndexTarget oldEvent,
			ExactIndexTarget oldMention,
			ExactIndexTarget newEvent,
			ExactIndexTarget newMention
	) {

		/** Проверяет обязательную новую coherent пару и разные physical names. */
		public AliasCutover {
			Objects.requireNonNull(newEvent, "newEvent must not be null");
			Objects.requireNonNull(newMention, "newMention must not be null");
			if (!GdeltIndexKind.EVENT.accepts(newEvent)
					|| !GdeltIndexKind.MENTION.accepts(newMention)) {
				throw new IllegalArgumentException("new targets must be an Event/Mention pair");
			}
			if (oldEvent != null && !GdeltIndexKind.EVENT.accepts(oldEvent)) {
				throw new IllegalArgumentException("oldEvent must be an Event target");
			}
			if (oldMention != null && !GdeltIndexKind.MENTION.accepts(oldMention)) {
				throw new IllegalArgumentException("oldMention must be a Mention target");
			}
			if ((oldEvent != null && oldEvent.indexName().equals(newEvent.indexName()))
					|| (oldMention != null
						&& oldMention.indexName().equals(newMention.indexName()))) {
				throw new IllegalArgumentException("old and new targets must differ");
			}
		}
	}

	private static void requireExact(String value, String field) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.isBlank()
				|| value.length() > 255
				|| value.indexOf('*') >= 0
				|| value.indexOf('?') >= 0
				|| value.indexOf(',') >= 0
				|| value.chars().anyMatch(Character::isWhitespace)) {
			throw new IllegalArgumentException(field + " must be a bounded exact value");
		}
	}
}
