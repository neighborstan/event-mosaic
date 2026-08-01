package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/**
 * Считает MD5 fingerprint файлов потоково, не загружая archive в память.
 */
final class Md5Checksum {

	private Md5Checksum() {
	}

	static MessageDigest newDigest() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("MD5 is not available in this Java runtime", exception);
		}
	}

	static String calculate(Path path) throws IOException {
		return calculate(path, OperationBudget.start(Duration.ofDays(1)));
	}

	static String calculate(Path path, OperationBudget budget) throws IOException {
		MessageDigest digest = newDigest();
		byte[] buffer = new byte[8192];
		try (InputStream input = Files.newInputStream(path)) {
			while (true) {
				IngestionInterruption.throwIfRequested();
				throwIfExpired(budget);
				int read = input.read(buffer);
				if (read == -1) {
					break;
				}
				digest.update(buffer, 0, read);
			}
		}
		IngestionInterruption.throwIfRequested();
		throwIfExpired(budget);
		return hex(digest);
	}

	private static void throwIfExpired(OperationBudget budget) {
		if (!budget.hasRemaining()) {
			throw new OperationDeadlineExceededException();
		}
	}

	static String hex(MessageDigest digest) {
		return HexFormat.of().formatHex(digest.digest());
	}
}
