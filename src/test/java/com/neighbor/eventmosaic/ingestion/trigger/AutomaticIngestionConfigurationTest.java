package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Выделенный worker автоматической загрузки")
class AutomaticIngestionConfigurationTest {

	@Test
	@DisplayName("Вторая задача ждет завершения первой на том же единственном worker")
	void dedicatedExecutorRunsOnlyOneTaskAtATime() throws Exception {
		ScheduledExecutorService executor = new AutomaticIngestionConfiguration()
				.automaticIngestionExecutor();
		CountDownLatch firstStarted = new CountDownLatch(1);
		CountDownLatch releaseFirst = new CountDownLatch(1);
		CountDownLatch secondStarted = new CountDownLatch(1);
		AtomicReference<Thread> firstThread = new AtomicReference<>();
		AtomicReference<Thread> secondThread = new AtomicReference<>();
		try {
			executor.submit(() -> {
				firstThread.set(Thread.currentThread());
				firstStarted.countDown();
				try {
					releaseFirst.await();
				}
				catch (InterruptedException exception) {
					Thread.currentThread().interrupt();
				}
			});
			executor.submit(() -> {
				secondThread.set(Thread.currentThread());
				secondStarted.countDown();
			});

			assertThat(firstStarted.await(1, TimeUnit.SECONDS)).isTrue();
			assertThat(secondStarted.await(50, TimeUnit.MILLISECONDS)).isFalse();
			releaseFirst.countDown();
			assertThat(secondStarted.await(1, TimeUnit.SECONDS)).isTrue();
			assertThat(secondThread.get()).isSameAs(firstThread.get());
			assertThat(firstThread.get().getName()).startsWith("gdelt-automatic-");
		}
		finally {
			releaseFirst.countDown();
			executor.shutdownNow();
			assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
		}
	}
}
