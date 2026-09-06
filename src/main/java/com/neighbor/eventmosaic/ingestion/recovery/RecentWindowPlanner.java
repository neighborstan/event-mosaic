package com.neighbor.eventmosaic.ingestion.recovery;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.time.RollingWindowPolicy;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import org.springframework.stereotype.Component;

/** Строит план восстановления данных GDELT за последние завершенные сутки. */
@Component
public final class RecentWindowPlanner {

	private final Clock clock;
	private final RollingWindowPolicy windowPolicy;

	/** Создает планировщик на общих серверных часах и правиле расчета суточного окна карты. */
	public RecentWindowPlanner(Clock clock, RollingWindowPolicy windowPolicy) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.windowPolicy = Objects.requireNonNull(windowPolicy, "windowPolicy must not be null");
		if (!GdeltSourceContract.UPDATE_INTERVAL.equals(windowPolicy.cadence())) {
			throw new IllegalArgumentException("windowPolicy cadence must match GDELT");
		}
	}

	/**
	 * Строит 96 пятнадцатиминутных отрезков суточного окна, добавляет самое новое обновление и промежуток между ними.
	 * Если указанное последнее обновление оказалось позже уже наступившей 15-минутной границы, метод отклоняет противоречивые данные источника.
	 */
	public RecentWindowPlan plan(Instant newestUpdateTime) {
		Objects.requireNonNull(newestUpdateTime, "newestUpdateTime must not be null");
		if (!GdeltSourceContract.isUpdateBoundary(newestUpdateTime)) {
			throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH);
		}
		Instant capturedNow = clock.instant();
		if (newestUpdateTime.isAfter(windowPolicy.floorToCadence(capturedNow))) {
			throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH);
		}
		var window = windowPolicy.windowAt(capturedNow);
		return RecentWindowPlan.fromBoundaries(window.from(), window.to(), newestUpdateTime);
	}
}
