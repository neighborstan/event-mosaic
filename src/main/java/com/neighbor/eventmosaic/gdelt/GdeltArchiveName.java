package com.neighbor.eventmosaic.gdelt;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Разобранное каноническое имя поддерживаемого GDELT Translation archive.
 *
 * @param value полное имя ZIP-файла
 * @param updateTime время source update в UTC
 * @param kind тип архива
 */
public record GdeltArchiveName(String value, Instant updateTime, GdeltArchiveKind kind) {

	private static final Pattern SUPPORTED_NAME = Pattern.compile(
			"^(\\d{14})\\.translation\\.(export|mentions)\\.CSV\\.zip$");
	private static final String ZIP_SUFFIX = ".zip";

	/**
	 * Проверяет согласованность имени, UTC timestamp и типа архива.
	 */
	public GdeltArchiveName {
		Objects.requireNonNull(value, "value must not be null");
		Objects.requireNonNull(updateTime, "updateTime must not be null");
		Objects.requireNonNull(kind, "kind must not be null");
		Matcher matcher = SUPPORTED_NAME.matcher(value);
		if (!matcher.matches()
				|| !GdeltSourceContract.parseUpdateTimestamp(matcher.group(1)).equals(updateTime)
				|| GdeltArchiveKind.fromManifestSuffix(matcher.group(2)) != kind) {
			throw new IllegalArgumentException("GDELT archive name, timestamp and kind must be consistent");
		}
	}

	/**
	 * Пытается разобрать имя поддерживаемого Event или Mention archive.
	 *
	 * @param value имя файла из manifest
	 * @return разобранное имя либо empty для неподдерживаемого типа archive
	 * @throws IllegalArgumentException если поддерживаемое имя содержит некорректный timestamp
	 */
	public static Optional<GdeltArchiveName> parseSupported(String value) {
		Objects.requireNonNull(value, "value must not be null");
		Matcher matcher = SUPPORTED_NAME.matcher(value);
		if (!matcher.matches()) {
			return Optional.empty();
		}
		return Optional.of(new GdeltArchiveName(
				value,
				GdeltSourceContract.parseUpdateTimestamp(matcher.group(1)),
				GdeltArchiveKind.fromManifestSuffix(matcher.group(2))
		));
	}

	/**
	 * Разбирает имя обязательного поддерживаемого archive.
	 *
	 * @param value имя ZIP-файла
	 * @return валидированное имя archive
	 * @throws IllegalArgumentException если имя не поддерживается или некорректно
	 */
	public static GdeltArchiveName requireSupported(String value) {
		return parseSupported(value)
				.orElseThrow(() -> new IllegalArgumentException("Unsupported GDELT Translation archive name"));
	}

	/**
	 * Возвращает ожидаемое имя единственного CSV entry внутри ZIP.
	 *
	 * @return имя archive без суффикса {@code .zip}
	 */
	public String csvName() {
		return value.substring(0, value.length() - ZIP_SUFFIX.length());
	}
}
