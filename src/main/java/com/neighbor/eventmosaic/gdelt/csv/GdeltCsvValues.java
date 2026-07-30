package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Строго преобразует provider values без trim и без молчаливой замены
 * malformed непустых значений на {@code null}.
 *
 * <p>Явный диапазон {@code [0-9]} фиксирует ASCII-контракт provider values и
 * намеренно не заменяется на зависящий от regex flags класс {@code \d}.</p>
 */
@SuppressWarnings("java:S6353")
final class GdeltCsvValues {

	private static final Pattern SIGNED_INTEGER = Pattern.compile("[+-]?[0-9]+");
	private static final Pattern UNSIGNED_8_DIGITS = Pattern.compile("[0-9]{8}");
	private static final Pattern UNSIGNED_6_DIGITS = Pattern.compile("[0-9]{6}");
	private static final Pattern UNSIGNED_14_DIGITS = Pattern.compile("[0-9]{14}");
	private static final DateTimeFormatter DAY = strictFormatter("uuuuMMdd");
	private static final DateTimeFormatter MONTH = strictFormatter("uuuuMM");
	private static final DateTimeFormatter TIMESTAMP = strictFormatter("uuuuMMddHHmmss");

	private GdeltCsvValues() {
	}

	static long requiredLong(String value) {
		if (requireValue(value).isEmpty()) {
			throw rejected(GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING);
		}
		return parseLong(value);
	}

	static String requiredString(String value) {
		if (requireValue(value).isBlank()) {
			throw rejected(GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING);
		}
		return value;
	}

	static String rawString(String value) {
		return requireValue(value);
	}

	static Integer nullableInteger(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		if (!SIGNED_INTEGER.matcher(value).matches()) {
			throw invalid();
		}
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException _) {
			throw invalid();
		}
	}

	static Long nullableLong(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		return parseLong(value);
	}

	static Double nullableDecimal(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		try {
			double parsed = new BigDecimal(value).doubleValue();
			if (!Double.isFinite(parsed)) {
				throw invalid();
			}
			return parsed;
		} catch (NumberFormatException _) {
			throw invalid();
		}
	}

	static LocalDate nullableDay(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		if (!UNSIGNED_8_DIGITS.matcher(value).matches()) {
			throw invalid();
		}
		try {
			return LocalDate.parse(value, DAY);
		} catch (DateTimeException _) {
			throw invalid();
		}
	}

	static YearMonth nullableMonth(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		if (!UNSIGNED_6_DIGITS.matcher(value).matches()) {
			throw invalid();
		}
		try {
			return YearMonth.parse(value, MONTH);
		} catch (DateTimeException _) {
			throw invalid();
		}
	}

	static Instant nullableTimestamp(String value) {
		if (requireValue(value).isEmpty()) {
			return null;
		}
		if (!UNSIGNED_14_DIGITS.matcher(value).matches()) {
			throw invalid();
		}
		try {
			return LocalDateTime.parse(value, TIMESTAMP).toInstant(ZoneOffset.UTC);
		} catch (DateTimeException _) {
			throw invalid();
		}
	}

	private static long parseLong(String value) {
		if (!SIGNED_INTEGER.matcher(value).matches()) {
			throw invalid();
		}
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException _) {
			throw invalid();
		}
	}

	private static String requireValue(String value) {
		return Objects.requireNonNull(value, "CSV field must not be null");
	}

	private static GdeltCsvValueException invalid() {
		return rejected(GdeltCsvRecordErrorCode.INVALID_VALUE);
	}

	private static GdeltCsvValueException rejected(GdeltCsvRecordErrorCode reason) {
		return new GdeltCsvValueException(reason);
	}

	private static DateTimeFormatter strictFormatter(String pattern) {
		return new DateTimeFormatterBuilder()
				.appendPattern(pattern)
				.toFormatter(Locale.ROOT)
				.withResolverStyle(ResolverStyle.STRICT);
	}
}
