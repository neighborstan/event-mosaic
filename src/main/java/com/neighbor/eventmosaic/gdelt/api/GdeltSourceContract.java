package com.neighbor.eventmosaic.gdelt.api;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Канонический технический контракт источника GDELT: адреса, периодичность,
 * UTC timestamp и формат MD5 из manifest.
 */
public final class GdeltSourceContract {

	/** Каноническое имя источника в ingestion ledger и telemetry. */
	public static final String SOURCE_NAME = "GDELT";

	/**
	 * Официальный HTTPS-каталог объектов GDELT v2.
	 *
	 * <p>URI является фиксированным trust anchor, а runtime property обязана
	 * совпадать с ним и не расширяет allowlist.</p>
	 */
	@SuppressWarnings("java:S1075")
	public static final URI OFFICIAL_DOWNLOAD_BASE_URI =
			URI.create("https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/");

	/** Период публикации update GDELT. */
	public static final Duration UPDATE_INTERVAL = Duration.ofMinutes(15);

	/** Имя manifest с последним GDELT Translation update. */
	public static final String LATEST_TRANSLATION_MANIFEST = "lastupdate-translation.txt";

	private static final String METADATA_SCHEME = "http";
	private static final String METADATA_HOST = "data.gdeltproject.org";
	private static final String METADATA_PATH_PREFIX = "/gdeltv2/";
	private static final Pattern MD5 = Pattern.compile("[0-9a-fA-F]{32}");
	private static final DateTimeFormatter UPDATE_TIMESTAMP = new DateTimeFormatterBuilder()
			.appendPattern("uuuuMMddHHmmss")
			.toFormatter(Locale.ROOT)
			.withResolverStyle(ResolverStyle.STRICT)
			.withZone(ZoneOffset.UTC);

	private GdeltSourceContract() {
	}

	/**
	 * Проверяет официальный download base URI и возвращает его без преобразований.
	 *
	 * @param baseUri проверяемый URI
	 * @return тот же канонический URI
	 * @throws IllegalArgumentException если URI не является официальным GCS-каталогом GDELT
	 */
	public static URI requireOfficialDownloadBaseUri(URI baseUri) {
		Objects.requireNonNull(baseUri, "baseUri must not be null");
		if (!OFFICIAL_DOWNLOAD_BASE_URI.equals(baseUri)) {
			throw new IllegalArgumentException("baseUri must be the official GDELT HTTPS GCS directory URI");
		}
		return baseUri;
	}

	/**
	 * Извлекает единственное имя файла из официального metadata URI.
	 *
	 * @param metadataUri URI из manifest
	 * @return имя файла без каталога
	 * @throws IllegalArgumentException если URI выходит за allowlist GDELT metadata
	 */
	public static String requireMetadataFileName(URI metadataUri) {
		Objects.requireNonNull(metadataUri, "metadataUri must not be null");
		String path = metadataUri.getPath();
		if (!METADATA_SCHEME.equalsIgnoreCase(metadataUri.getScheme())
				|| !METADATA_HOST.equalsIgnoreCase(metadataUri.getHost())
				|| metadataUri.getPort() != -1
				|| metadataUri.getUserInfo() != null
				|| metadataUri.getQuery() != null
				|| metadataUri.getFragment() != null
				|| path == null
				|| !path.equals(metadataUri.getRawPath())
				|| !path.startsWith(METADATA_PATH_PREFIX)
				|| path.length() == METADATA_PATH_PREFIX.length()
				|| path.indexOf('/', METADATA_PATH_PREFIX.length()) >= 0) {
			throw new IllegalArgumentException("metadataUri is outside the GDELT metadata allowlist");
		}
		return path.substring(METADATA_PATH_PREFIX.length());
	}

	/**
	 * Нормализует MD5 из manifest в lower-case ASCII.
	 *
	 * @param checksum проверяемая checksum
	 * @return нормализованные 32 шестнадцатеричных символа
	 * @throws IllegalArgumentException если формат checksum не соответствует контракту GDELT
	 */
	public static String normalizeMd5(String checksum) {
		Objects.requireNonNull(checksum, "checksum must not be null");
		if (!MD5.matcher(checksum).matches()) {
			throw new IllegalArgumentException("checksum must contain 32 hexadecimal characters");
		}
		return checksum.toLowerCase(Locale.ROOT);
	}

	/**
	 * Разбирает строгий 14-значный UTC timestamp из имени GDELT archive.
	 *
	 * @param timestamp timestamp формата {@code uuuuMMddHHmmss}
	 * @return момент времени в UTC
	 * @throws IllegalArgumentException если timestamp некорректен или не попадает на 15-минутную границу
	 */
	public static Instant parseUpdateTimestamp(String timestamp) {
		Objects.requireNonNull(timestamp, "timestamp must not be null");
		try {
			Instant updateTime = LocalDateTime.parse(timestamp, UPDATE_TIMESTAMP).toInstant(ZoneOffset.UTC);
			if (!isUpdateBoundary(updateTime)) {
				throw new IllegalArgumentException("GDELT update timestamp must align to a 15-minute UTC boundary");
			}
			return updateTime;
		} catch (DateTimeParseException exception) {
			throw new IllegalArgumentException("GDELT update timestamp must be a valid UTC timestamp", exception);
		}
	}

	/**
	 * Форматирует момент публикации в канонический UTC timestamp GDELT.
	 *
	 * @param updateTime момент публикации
	 * @return строка формата {@code uuuuMMddHHmmss}
	 */
	public static String formatUpdateTimestamp(Instant updateTime) {
		Objects.requireNonNull(updateTime, "updateTime must not be null");
		return UPDATE_TIMESTAMP.format(updateTime);
	}

	/**
	 * Проверяет попадание момента на границу update GDELT.
	 *
	 * @param updateTime проверяемый момент
	 * @return {@code true}, если timestamp имеет секундную точность и кратен 15 минутам
	 */
	public static boolean isUpdateBoundary(Instant updateTime) {
		Objects.requireNonNull(updateTime, "updateTime must not be null");
		return updateTime.getNano() == 0
				&& Math.floorMod(updateTime.getEpochSecond(), UPDATE_INTERVAL.toSeconds()) == 0;
	}
}
