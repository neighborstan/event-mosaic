package com.neighbor.eventmosaic.gdelt.api;

/**
 * Поддерживаемые типы архивов GDELT Translation, входящие в один update.
 */
public enum GdeltArchiveKind {

	TRANSLATION_EVENTS("events"),
	TRANSLATION_MENTIONS("mentions");

	private final String stagingDirectory;

	GdeltArchiveKind(String stagingDirectory) {
		this.stagingDirectory = stagingDirectory;
	}

	/**
	 * Возвращает стабильное имя каталога типа архива в staging.
	 *
	 * @return имя каталога без разделителей пути
	 */
	public String stagingDirectory() {
		return stagingDirectory;
	}

	static GdeltArchiveKind fromManifestSuffix(String suffix) {
		return switch (suffix) {
			case "export" -> TRANSLATION_EVENTS;
			case "mentions" -> TRANSLATION_MENTIONS;
			default -> throw new IllegalArgumentException("Unsupported GDELT archive suffix: " + suffix);
		};
	}
}
