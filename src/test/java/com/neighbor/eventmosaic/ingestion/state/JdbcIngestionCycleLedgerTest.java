package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

@DisplayName("Ограничение времени запросов общего ingestion cycle")
class JdbcIngestionCycleLedgerTest {

	private static final String SOURCE_NAME = GdeltSourceContract.SOURCE_NAME;
	private static final Duration LEASE = Duration.ofMinutes(15);
	private static final IngestionCycleOwnership OWNERSHIP =
			new IngestionCycleOwnership(
					SOURCE_NAME,
					UUID.fromString("11111111-1111-1111-1111-111111111111"),
					7,
					Instant.parse("2026-08-20T12:15:00Z"));

	private final JdbcIngestionCycleRepository repository =
			mock(JdbcIngestionCycleRepository.class);
	private final PlatformTransactionManager transactionManager =
			mock(PlatformTransactionManager.class);
	private final TransactionStatus transactionStatus = mock(TransactionStatus.class);

	private JdbcIngestionCycleLedger ledger;

	@BeforeEach
	void setUp() {
		when(transactionManager.getTransaction(any(TransactionDefinition.class)))
				.thenReturn(transactionStatus);
		ledger = new JdbcIngestionCycleLedger(
				repository,
				transactionManager,
				Duration.ofSeconds(30));
	}

	@Test
	@DisplayName("Запрос не получает больше тридцати секунд при большом остатке cycle")
	void configuredTimeoutCapsLongBudget() {
		when(repository.claim(SOURCE_NAME, LEASE)).thenReturn(Optional.empty());

		ledger.claim(
				SOURCE_NAME,
				LEASE,
				OperationBudget.start(Duration.ofSeconds(45), () -> 0));

		TransactionDefinition definition = transactionDefinition();
		assertThat(definition.getTimeout()).isEqualTo(30);
		assertThat(definition.isReadOnly()).isFalse();
		verify(transactionManager).commit(transactionStatus);
	}

	@Test
	@DisplayName("Неполная секунда остатка не добавляется к timeout транзакции")
	void remainingBudgetIsRoundedDownToWholeSeconds() {
		when(repository.remainingLease(OWNERSHIP)).thenReturn(Optional.of(LEASE));

		ledger.remainingLease(
				OWNERSHIP,
				OperationBudget.start(Duration.ofMillis(2_500), () -> 0));

		TransactionDefinition definition = transactionDefinition();
		assertThat(definition.getTimeout()).isEqualTo(2);
		assertThat(definition.isReadOnly()).isTrue();
	}

	@Test
	@DisplayName("Остаток меньше секунды останавливается до обращения к PostgreSQL")
	void subSecondBudgetDoesNotStartDatabaseWork() {
		OperationBudget budget = OperationBudget.start(Duration.ofMillis(999), () -> 0);

		assertThatThrownBy(() -> ledger.claim(SOURCE_NAME, LEASE, budget))
				.isInstanceOf(OperationDeadlineReachedException.class);

		verifyNoInteractions(repository);
		verify(transactionManager, never())
				.getTransaction(any(TransactionDefinition.class));
	}

	@Test
	@DisplayName("Истекшая за время проверки deadline запрещает следующий шаг")
	void remainingLeaseRechecksDeadlineAfterCommit() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(
				Duration.ofSeconds(2),
				nanoTime::get);
		when(repository.remainingLease(OWNERSHIP)).thenAnswer(_ -> {
			nanoTime.set(Duration.ofSeconds(2).toNanos());
			return Optional.of(LEASE);
		});

		assertThatThrownBy(() -> ledger.remainingLease(OWNERSHIP, budget))
				.isInstanceOf(OperationDeadlineReachedException.class);

		verify(transactionManager).commit(transactionStatus);
	}

	@Test
	@DisplayName("Полученное владение возвращается даже при истекшей после commit deadline")
	void acquiredOwnershipRemainsVisibleAfterLateCommit() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(
				Duration.ofSeconds(2),
				nanoTime::get);
		when(repository.claim(SOURCE_NAME, LEASE)).thenAnswer(_ -> {
			nanoTime.set(Duration.ofSeconds(2).toNanos());
			return Optional.of(OWNERSHIP);
		});

		assertThat(ledger.claim(SOURCE_NAME, LEASE, budget))
				.contains(OWNERSHIP);
		assertThat(budget.hasRemaining()).isFalse();
	}

	@Test
	@DisplayName("Поздний ответ без владения не маскирует истекшую deadline")
	void emptyLateClaimReportsDeadline() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(
				Duration.ofSeconds(2),
				nanoTime::get);
		when(repository.claim(SOURCE_NAME, LEASE)).thenAnswer(_ -> {
			nanoTime.set(Duration.ofSeconds(2).toNanos());
			return Optional.empty();
		});

		assertThatThrownBy(() -> ledger.claim(SOURCE_NAME, LEASE, budget))
				.isInstanceOf(OperationDeadlineReachedException.class);
	}

	private TransactionDefinition transactionDefinition() {
		ArgumentCaptor<TransactionDefinition> definition =
				ArgumentCaptor.forClass(TransactionDefinition.class);
		verify(transactionManager).getTransaction(definition.capture());
		return definition.getValue();
	}
}
