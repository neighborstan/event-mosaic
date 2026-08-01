package com.neighbor.eventmosaic.ingestion.retry;

import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/** Использует thread-local random source без общего contention между workers. */
@Component
final class RandomRetryJitterSource implements RetryJitterSource {

	@Override
	public double nextSample() {
		return ThreadLocalRandom.current().nextDouble();
	}
}
