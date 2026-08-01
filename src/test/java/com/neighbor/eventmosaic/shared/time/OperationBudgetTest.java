package com.neighbor.eventmosaic.shared.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
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
}
