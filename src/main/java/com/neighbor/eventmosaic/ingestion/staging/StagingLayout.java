package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.gdelt.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Детерминированно строит безопасные final и attempt-scoped staging paths.
 */
@Component
public class StagingLayout {

	private final Path root;

	/**
	 * Создает layout от настроенного staging root.
	 *
	 * @param properties runtime properties ingestion
	 */
	@Autowired
	public StagingLayout(GdeltIngestionProperties properties) {
		this(properties.stagingRoot());
	}

	/**
	 * Создает layout от явно заданного root для package-level composition.
	 *
	 * @param root доверенный staging root
	 */
	StagingLayout(Path root) {
		this.root = root.toAbsolutePath().normalize();
	}

	/**
	 * Строит paths для конкретного archive attempt и проверяет containment в root.
	 *
	 * @param attempt archive и уникальный token владения
	 * @return final и temporary paths для ZIP и CSV
	 */
	public StagingPaths pathsFor(ArchiveAttempt attempt) {
		GdeltArchiveName archiveName = GdeltArchiveName.requireSupported(attempt.archive().archiveName());
		Path archiveRoot = root
				.resolve(GdeltSourceContract.formatUpdateTimestamp(archiveName.updateTime()))
				.resolve(archiveName.kind().stagingDirectory())
				.normalize();
		String archiveFileName = archiveName.value();
		String csvName = archiveName.csvName();
		String token = attempt.token().toString();
		Path archivePath = archiveRoot.resolve("archive").resolve(archiveFileName).normalize();
		Path csvPath = archiveRoot.resolve("data").resolve(csvName).normalize();
		return new StagingPaths(
				root,
				archivePath,
				archivePath.resolveSibling(archiveFileName + "." + token + ".part"),
				csvPath,
				csvPath.resolveSibling(csvName + "." + token + ".part")
		);
	}
}
