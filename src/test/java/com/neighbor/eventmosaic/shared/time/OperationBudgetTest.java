package com.neighbor.eventmosaic.shared.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Общий monotonic operation budget")
class OperationBudgetTest {

	@Test
	@DisplayName("Перевод wall clock вперед и назад не меняет остаток budget")
	void wallClockJumpDoesNotChangeRemainingBudget() {
		AtomicLong nanoTime = new AtomicLong(1_000_000L);
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(12), nanoTime::get);
		Clock initialClock = Clock.fixed(
				Instant.parse("2026-08-01T12:00:00Z"),
				ZoneOffset.UTC);

		Clock.offset(initialClock, Duration.ofDays(30)).instant();
		Clock.offset(initialClock, Duration.ofDays(-30)).instant();
		assertThat(budget.remaining()).isEqualTo(Duration.ofMinutes(12));

		nanoTime.addAndGet(Duration.ofMinutes(2).toNanos());
		assertThat(budget.remaining()).isEqualTo(Duration.ofMinutes(10));
	}

	@Test
	@DisplayName("Child timeout ограничивается остатком и становится zero на deadline")
	void childTimeoutIsCappedByRemainingBudget() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(1), nanoTime::get);

		assertThat(budget.cap(Duration.ofMinutes(2))).isEqualTo(Duration.ofMinutes(1));
		nanoTime.addAndGet(Duration.ofSeconds(45).toNanos());
		assertThat(budget.cap(Duration.ofMinutes(2))).isEqualTo(Duration.ofSeconds(15));
		nanoTime.addAndGet(Duration.ofSeconds(15).toNanos());
		assertThat(budget.cap(Duration.ofMinutes(2))).isZero();
		assertThat(budget.hasRemaining()).isFalse();
	}

	@Test
	@DisplayName("Child timeout ограничивается безопасным остатком внешнего lease")
	void childTimeoutIsCappedBySafeLeaseRemaining() {
		AtomicLong nanoTime = new AtomicLong();
		AtomicReference<OperationLeaseSnapshot> lease = new AtomicReference<>(
				OperationLeaseSnapshot.current(Duration.ofMinutes(5)));
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(12), nanoTime::get)
				.withLeaseGuard(lease::get, Duration.ofMinutes(3));

		assertThat(budget.checkpoint().status())
				.isEqualTo(OperationBudgetStatus.AVAILABLE);
		assertThat(budget.cap(Duration.ofMinutes(4)))
				.isEqualTo(Duration.ofMinutes(2));

		lease.set(OperationLeaseSnapshot.current(Duration.ofMinutes(3)));

		assertThat(budget.checkpoint().status())
				.isEqualTo(OperationBudgetStatus.OWNERSHIP_LOST);
		assertThat(budget.cap(Duration.ofMinutes(2))).isZero();
	}

	@Test
	@DisplayName("Медленная ownership-проверка повторно проверяет общую deadline")
	void slowOwnershipCheckCannotReturnStaleDeadline() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget
				.start(Duration.ofSeconds(5), nanoTime::get)
				.withLeaseGuard(
						() -> {
							nanoTime.set(Duration.ofSeconds(5).toNanos());
							return OperationLeaseSnapshot.current(Duration.ofMinutes(10));
						},
						Duration.ofMinutes(3));

		assertThat(budget.checkpoint())
				.isEqualTo(new OperationBudgetCheckpoint(
						OperationBudgetStatus.DEADLINE_EXCEEDED,
						Duration.ZERO));
	}

	@Test
	@DisplayName("Время ownership-проверки консервативно вычитается из lease")
	void ownershipCheckTimeIsSubtractedFromLease() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(12), nanoTime::get)
				.withLeaseGuard(
						() -> {
							nanoTime.addAndGet(Duration.ofSeconds(2).toNanos());
							return OperationLeaseSnapshot.current(Duration.ofSeconds(10));
						},
						Duration.ofSeconds(3));

		assertThat(budget.checkpoint())
				.isEqualTo(new OperationBudgetCheckpoint(
						OperationBudgetStatus.AVAILABLE,
						Duration.ofSeconds(5)));
	}

	@Test
	@DisplayName("Потерянный ownership запрещает следующий шаг до дочернего вызова")
	void lostOwnershipStopsBudget() {
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(12), () -> 0L)
				.withLeaseGuard(
						OperationLeaseSnapshot::lost,
						Duration.ofMinutes(3));

		assertThat(budget.hasRemaining()).isFalse();
		assertThat(budget.checkpoint())
				.isEqualTo(new OperationBudgetCheckpoint(
						OperationBudgetStatus.OWNERSHIP_LOST,
						Duration.ZERO));
	}

	@Test
	@DisplayName("Ошибка проверки ownership распространяется и запрещает продолжение")
	void ownershipCheckFailureIsNotTreatedAsAvailable() {
		IllegalStateException failure = new IllegalStateException("database unavailable");
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(12), () -> 0L)
				.withLeaseGuard(
						() -> {
							throw failure;
						},
						Duration.ofMinutes(3));

		assertThatThrownBy(budget::hasRemaining)
				.isSameAs(failure);
	}

	@Test
	@DisplayName("Обязательная проверка различает deadline и потерю ownership")
	void requiredCheckpointKeepsFailureReasonsDistinct() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget expired = OperationBudget.start(
				Duration.ofSeconds(1),
				nanoTime::get);
		nanoTime.set(Duration.ofSeconds(1).toNanos());
		OperationBudget lost = OperationBudget
				.start(Duration.ofMinutes(12), () -> 0L)
				.withLeaseGuard(
						OperationLeaseSnapshot::lost,
						Duration.ofMinutes(3));

		assertThatThrownBy(expired::requireAvailable)
				.isInstanceOf(OperationDeadlineReachedException.class);
		assertThatThrownBy(lost::requireAvailable)
				.isInstanceOf(OperationOwnershipLostException.class);
	}

	@Test
	@DisplayName("Effective timeout сообщает настроенное, deadline и lease ограничение")
	void effectiveTimeoutReportsItsOrigin() {
		OperationBudget configured = OperationBudget.start(
				Duration.ofMinutes(12),
				() -> 0L);
		OperationBudget deadline = OperationBudget.start(
				Duration.ofMinutes(1),
				() -> 0L);
		OperationBudget lease = OperationBudget
				.start(Duration.ofMinutes(12), () -> 0L)
				.withLeaseGuard(
						() -> OperationLeaseSnapshot.current(Duration.ofMinutes(5)),
						Duration.ofMinutes(3));

		assertThat(configured.effectiveTimeout(Duration.ofMinutes(1)))
				.isEqualTo(new OperationEffectiveTimeout(
						Duration.ofMinutes(1),
						OperationTimeoutOrigin.CONFIGURED_TIMEOUT));
		assertThat(deadline.effectiveTimeout(Duration.ofMinutes(2)))
				.isEqualTo(new OperationEffectiveTimeout(
						Duration.ofMinutes(1),
						OperationTimeoutOrigin.OPERATION_DEADLINE));
		assertThat(lease.effectiveTimeout(Duration.ofMinutes(4)))
				.isEqualTo(new OperationEffectiveTimeout(
						Duration.ofMinutes(2),
						OperationTimeoutOrigin.LEASE_SAFETY));
		assertThat(lease.effectiveTimeout(Duration.ofMinutes(2)).origin())
				.isEqualTo(OperationTimeoutOrigin.LEASE_SAFETY);
	}

	@Test
	@DisplayName("Повторная проверка timeout отдает приоритет потере lease и deadline")
	void timeoutRecheckUsesActualStatePriority() {
		AtomicLong leaseNanos = new AtomicLong();
		AtomicReference<OperationLeaseSnapshot> leaseState = new AtomicReference<>(
				OperationLeaseSnapshot.current(Duration.ofMinutes(10)));
		OperationBudget leaseBudget = OperationBudget
				.start(Duration.ofSeconds(5), leaseNanos::get)
				.withLeaseGuard(leaseState::get, Duration.ZERO);
		OperationEffectiveTimeout attempted = leaseBudget.effectiveTimeout(
				Duration.ofSeconds(1));
		leaseNanos.set(Duration.ofSeconds(5).toNanos());
		leaseState.set(OperationLeaseSnapshot.lost());

		assertThat(leaseBudget.resolveTimeoutOrigin(attempted))
				.isEqualTo(OperationTimeoutOrigin.LEASE_SAFETY);

		AtomicLong deadlineNanos = new AtomicLong();
		OperationBudget deadlineBudget = OperationBudget.start(
				Duration.ofSeconds(5),
				deadlineNanos::get);
		OperationEffectiveTimeout deadlineAttempt = deadlineBudget.effectiveTimeout(
				Duration.ofSeconds(1));
		deadlineNanos.set(Duration.ofSeconds(5).toNanos());

		assertThat(deadlineBudget.resolveTimeoutOrigin(deadlineAttempt))
				.isEqualTo(OperationTimeoutOrigin.OPERATION_DEADLINE);

		OperationBudget available = OperationBudget.start(
				Duration.ofSeconds(5),
				() -> 0L);
		OperationEffectiveTimeout configuredAttempt = available.effectiveTimeout(
				Duration.ofSeconds(1));
		assertThat(available.resolveTimeoutOrigin(configuredAttempt))
				.isEqualTo(OperationTimeoutOrigin.CONFIGURED_TIMEOUT);
	}

	@Test
	@DisplayName("После внешнего ответа потеря ownership важнее одновременно истекшей deadline")
	void externalResultRecheckUsesOwnershipFirst() {
		AtomicLong nanoTime = new AtomicLong();
		AtomicInteger leaseChecks = new AtomicInteger();
		OperationBudget lost = OperationBudget
				.start(Duration.ofSeconds(1), nanoTime::get)
				.withLeaseGuard(
						() -> {
							leaseChecks.incrementAndGet();
							return OperationLeaseSnapshot.lost();
						},
						Duration.ZERO);
		nanoTime.set(Duration.ofSeconds(1).toNanos());

		assertThatThrownBy(lost::requireAvailableAfterExternalResult)
				.isInstanceOf(OperationOwnershipLostException.class);
		assertThat(leaseChecks).hasValue(1);

		OperationBudget current = OperationBudget
				.start(Duration.ofSeconds(1), nanoTime::get)
				.withLeaseGuard(
						() -> OperationLeaseSnapshot.current(Duration.ofMinutes(1)),
						Duration.ZERO);
		nanoTime.addAndGet(Duration.ofSeconds(1).toNanos());

		assertThatThrownBy(current::requireAvailableAfterExternalResult)
				.isInstanceOf(OperationDeadlineReachedException.class);
	}

	@Test
	@DisplayName("Terminal-компенсация проходит после deadline только у текущего владельца")
	void terminalTransitionIgnoresDeadlineButNotOwnership() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget current = OperationBudget
				.start(Duration.ofMinutes(12), nanoTime::get)
				.withLeaseGuard(
						() -> OperationLeaseSnapshot.current(Duration.ofMinutes(3)),
						Duration.ofMinutes(3));
		nanoTime.set(Duration.ofMinutes(12).toNanos());

		assertThatCode(current::requireOwnershipForTerminalTransition)
				.doesNotThrowAnyException();

		OperationBudget lost = OperationBudget
				.start(Duration.ofMinutes(12), nanoTime::get)
				.withLeaseGuard(
						OperationLeaseSnapshot::lost,
						Duration.ofMinutes(3));
		nanoTime.addAndGet(Duration.ofMinutes(12).toNanos());

		assertThatThrownBy(lost::requireOwnershipForTerminalTransition)
				.isInstanceOf(OperationOwnershipLostException.class);
	}

	@Test
	@DisplayName("Terminal-компенсация не проходит при ошибке свежей проверки lease")
	void terminalTransitionFailsClosedWhenLeaseCheckFails() {
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(12), () -> 0L)
				.withLeaseGuard(
						() -> {
							throw new IllegalStateException("lease storage unavailable");
						},
						Duration.ofMinutes(3));

		assertThatThrownBy(budget::requireOwnershipForTerminalTransition)
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("lease storage unavailable");
	}

	@Test
	@DisplayName("Потоковый цикл обновляет lease раз в секунду и у safe boundary")
	void loopAllowanceRefreshesAtBoundedCadence() {
		AtomicLong nanoTime = new AtomicLong();
		AtomicInteger leaseChecks = new AtomicInteger();
		OperationBudget budget = OperationBudget
				.start(Duration.ofSeconds(30), nanoTime::get)
				.withLeaseGuard(
						() -> {
							leaseChecks.incrementAndGet();
							return OperationLeaseSnapshot.current(Duration.ofSeconds(10));
						},
						Duration.ZERO);

		budget.requireAvailable();
		budget.requireLoopAvailable();
		nanoTime.set(Duration.ofMillis(999).toNanos());
		budget.requireLoopAvailable();
		assertThat(leaseChecks).hasValue(1);

		nanoTime.set(Duration.ofSeconds(1).toNanos());
		budget.requireLoopAvailable();
		assertThat(leaseChecks).hasValue(2);

		AtomicLong shortLeaseNanos = new AtomicLong();
		AtomicInteger shortLeaseChecks = new AtomicInteger();
		OperationBudget shortLease = OperationBudget
				.start(Duration.ofSeconds(30), shortLeaseNanos::get)
				.withLeaseGuard(
						() -> shortLeaseChecks.incrementAndGet() == 1
								? OperationLeaseSnapshot.current(Duration.ofMillis(500))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);
		shortLease.requireAvailable();
		shortLeaseNanos.set(Duration.ofMillis(499).toNanos());
		shortLease.requireLoopAvailable();
		assertThat(shortLeaseChecks).hasValue(1);

		shortLeaseNanos.set(Duration.ofMillis(500).toNanos());
		assertThatThrownBy(shortLease::requireLoopAvailable)
				.isInstanceOf(OperationOwnershipLostException.class);
		assertThat(shortLeaseChecks).hasValue(2);
	}

	@Test
	@DisplayName("Ошибка обновления lease запрещает внешний шаг и следующий refresh цикла")
	void leaseRefreshFailureIsFailClosed() {
		AtomicLong nanoTime = new AtomicLong();
		AtomicReference<RuntimeException> failure = new AtomicReference<>();
		OperationBudget budget = OperationBudget
				.start(Duration.ofSeconds(30), nanoTime::get)
				.withLeaseGuard(
						() -> {
							if (failure.get() != null) {
								throw failure.get();
							}
							return OperationLeaseSnapshot.current(Duration.ofSeconds(10));
						},
						Duration.ZERO);
		budget.requireAvailable();
		IllegalStateException databaseFailure = new IllegalStateException(
				"database unavailable");
		failure.set(databaseFailure);

		assertThatThrownBy(budget::requireAvailable).isSameAs(databaseFailure);

		nanoTime.set(Duration.ofSeconds(1).toNanos());
		assertThatThrownBy(budget::requireLoopAvailable).isSameAs(databaseFailure);
	}
}
