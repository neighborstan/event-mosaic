package com.neighbor.eventmosaic.gdelt.api;

/**
 * Классифицирует HTTP status источника GDELT для retry policy.
 */
public final class GdeltHttpStatusPolicy {

	private GdeltHttpStatusPolicy() {
	}

	/**
	 * Возвращает {@code true} только для временных ответов, которые имеет смысл
	 * повторить автоматически.
	 *
	 * @param statusCode HTTP status
	 * @return {@code true} для 408, 429 и 5xx
	 */
	public static boolean isTransient(int statusCode) {
		return statusCode == 408 || statusCode == 429 || statusCode >= 500 && statusCode <= 599;
	}
}
