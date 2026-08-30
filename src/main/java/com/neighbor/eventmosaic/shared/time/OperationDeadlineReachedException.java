package com.neighbor.eventmosaic.shared.time;

/**
 * Коротко останавливает вложенную работу после исчерпания общей monotonic
 * deadline. Владение внешним lease при этом не считается потерянным.
 */
public final class OperationDeadlineReachedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/** Создает внутренний control-flow сигнал без установки interrupt flag. */
	public OperationDeadlineReachedException() {
		super("Operation deadline was reached", null, true, false);
	}
}
