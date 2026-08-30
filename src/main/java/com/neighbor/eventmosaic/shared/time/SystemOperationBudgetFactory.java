package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import org.springframework.stereotype.Component;

/** Создает production budget поверх monotonic {@link System#nanoTime()}. */
@Component
final class SystemOperationBudgetFactory implements OperationBudgetFactory {

	@Override
	public OperationBudget start(Duration limit) {
		return OperationBudget.start(limit);
	}
}
