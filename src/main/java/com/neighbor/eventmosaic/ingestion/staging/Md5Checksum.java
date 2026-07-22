package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
		MessageDigest digest = newDigest();
		byte[] buffer = new byte[8192];
		try (InputStream input = Files.newInputStream(path)) {
			while (true) {
				IngestionInterruption.throwIfRequested();
				int read = input.read(buffer);
				if (read == -1) {
					break;
				}
				digest.update(buffer, 0, read);
			}
		}
		IngestionInterruption.throwIfRequested();
		return hex(digest);
	}

	static String hex(MessageDigest digest) {
		return HexFormat.of().formatHex(digest.digest());
	}
}
