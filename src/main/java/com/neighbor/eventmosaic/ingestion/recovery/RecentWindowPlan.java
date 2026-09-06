package com.neighbor.eventmosaic.ingestion.recovery;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Описывает, какие обновления GDELT нужно восстановить для карты. План включает последнее
 * завершенное суточное окно, самое новое известное обновление и, если нужно, промежуток между ними.
 *
 * @param windowFrom включаемое начало 24-часового окна
 * @param windowTo исключаемый конец 24-часового окна
 * @param sourceFrontier самое новое обновление, известное в начале цикла
 * @param targetUpdateTimes все нужные моменты публикации в порядке от старых к новым
 */
public record RecentWindowPlan(
		Instant windowFrom,
		Instant windowTo,
		Instant sourceFrontier,
		List<Instant> targetUpdateTimes
) {

	/** Копирует список моментов и проверяет, что он точно соответствует границам плана. */
	public RecentWindowPlan {
		Objects.requireNonNull(windowFrom, "windowFrom must not be null");
		Objects.requireNonNull(windowTo, "windowTo must not be null");
		Objects.requireNonNull(sourceFrontier, "sourceFrontier must not be null");
		targetUpdateTimes = List.copyOf(targetUpdateTimes);
		if (!GdeltSourceContract.isUpdateBoundary(windowFrom)
				|| !GdeltSourceContract.isUpdateBoundary(windowTo)
				|| !GdeltSourceContract.isUpdateBoundary(sourceFrontier)) {
			throw new IllegalArgumentException("recent plan timestamps must use the GDELT cadence");
		}
		if (!windowFrom.plusSeconds(24 * 60 * 60).equals(windowTo)) {
			throw new IllegalArgumentException("recent plan window must span exactly 24 hours");
		}
		if (!targetUpdateTimes.equals(targets(windowFrom, windowTo, sourceFrontier))) {
			throw new IllegalArgumentException("targetUpdateTimes do not match recent plan boundaries");
		}
	}

	/** Восстанавливает полный список нужных обновлений из ранее сохраненных границ плана. */
	public static RecentWindowPlan fromBoundaries(
			Instant windowFrom,
			Instant windowTo,
			Instant sourceFrontier
	) {
		return new RecentWindowPlan(
				windowFrom,
				windowTo,
				sourceFrontier,
				targets(windowFrom, windowTo, sourceFrontier));
	}

	private static List<Instant> targets(
			Instant windowFrom,
			Instant windowTo,
			Instant sourceFrontier
	) {
		var targets = new LinkedHashSet<Instant>();
		for (Instant slot = windowFrom; slot.isBefore(windowTo);
				slot = slot.plus(GdeltSourceContract.UPDATE_INTERVAL)) {
			targets.add(slot);
		}
		if (!sourceFrontier.isBefore(windowTo)) {
			for (Instant slot = windowTo; !slot.isAfter(sourceFrontier);
					slot = slot.plus(GdeltSourceContract.UPDATE_INTERVAL)) {
				targets.add(slot);
			}
		}
		targets.add(sourceFrontier);
		return new ArrayList<>(targets);
	}
}
