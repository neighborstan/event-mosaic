package com.neighbor.eventmosaic.ingestion.staging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Проверка доступности staging storage")
class StagingStorageProbeTest {

	@TempDir
	Path tempDir;

	@Test
	@DisplayName("Отсутствующий staging root безопасно создается и проходит проверку записи")
	void createsRootAndConfirmsWritableStorage() throws IOException {
		Path root = tempDir.resolve("staging");

		StagingStorageProbeResult result = new StagingStorageProbe(root).probe();

		assertThat(result.status()).isEqualTo(StagingStorageProbeStatus.WRITABLE);
		assertThat(result.errorCode()).isEqualTo(StagingStorageProbeErrorCode.NONE);
		assertThat(result.usableBytes()).isNotNegative();
		assertThat(root).isDirectory();
		try (var entries = Files.list(root)) {
			assertThat(entries).isEmpty();
		}
	}

	@Test
	@DisplayName("Обычный файл вместо staging root возвращает ограниченный отказ доступа")
	void returnsBoundedFailureForFileInsteadOfRoot() throws IOException {
		Path root = tempDir.resolve("staging");
		Files.writeString(root, "not a directory");

		StagingStorageProbeResult result = new StagingStorageProbe(root).probe();

		assertThat(result)
				.isEqualTo(StagingStorageProbeResult.unavailable(
						StagingStorageProbeErrorCode.ROOT_ACCESS_FAILED));
	}

	@Test
	@DisplayName("Symbolic link вместо staging root возвращает ограниченный unsafe status")
	void rejectsSymbolicLinkRoot() throws IOException {
		Path target = tempDir.resolve("target");
		Path root = tempDir.resolve("staging");
		Files.createDirectories(target);
		try {
			Files.createSymbolicLink(root, target);
		} catch (UnsupportedOperationException | SecurityException exception) {
			Assumptions.assumeTrue(false, "Filesystem does not support symbolic links");
		} catch (IOException exception) {
			Assumptions.assumeTrue(false, "Symbolic link creation is not permitted");
		}

		StagingStorageProbeResult result = new StagingStorageProbe(root).probe();

		assertThat(result)
				.isEqualTo(StagingStorageProbeResult.unsafe(
						StagingStorageProbeErrorCode.PATH_REJECTED));
	}
}
