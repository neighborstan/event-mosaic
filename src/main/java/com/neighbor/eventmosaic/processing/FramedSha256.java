package com.neighbor.eventmosaic.processing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Одноразово строит SHA-256 из однозначно framed primitive values.
 */
final class FramedSha256 {

	private static final String ALGORITHM = "SHA-256";

	private final MessageDigest digest;
	private boolean finished;

	private FramedSha256(MessageDigest digest) {
		this.digest = digest;
	}

	static FramedSha256 create() {
		try {
			return new FramedSha256(MessageDigest.getInstance(ALGORITHM));
		}
		catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("Required SHA-256 algorithm is unavailable", exception);
		}
	}

	FramedSha256 putString(String value) {
		if (value == null) {
			throw new IllegalArgumentException("framed string must not be null");
		}
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		putInt(bytes.length);
		digest.update(bytes);
		return this;
	}

	FramedSha256 putLong(long value) {
		requireOpen();
		for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
			digest.update((byte) (value >>> shift));
		}
		return this;
	}

	FramedSha256 putInt(int value) {
		requireOpen();
		for (int shift = Integer.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
			digest.update((byte) (value >>> shift));
		}
		return this;
	}

	String finishHex() {
		requireOpen();
		finished = true;
		return java.util.HexFormat.of().formatHex(digest.digest());
	}

	private void requireOpen() {
		if (finished) {
			throw new IllegalStateException("framed digest is already finished");
		}
	}
}
