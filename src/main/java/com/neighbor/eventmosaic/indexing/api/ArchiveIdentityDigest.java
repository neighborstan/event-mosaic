package com.neighbor.eventmosaic.indexing.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Канонический SHA-256 digest упорядоченного потока document identities архива.
 *
 * @param value 64 lowercase hexadecimal символа SHA-256
 */
public record ArchiveIdentityDigest(String value) {

	/** Версия алгоритма framing и хеширования archive identities. */
	public static final String ALGORITHM = "sha256-length-prefix-v1";

	private static final String SHA_256 = "SHA-256";
	private static final Pattern LOWERCASE_SHA_256 = Pattern.compile("[0-9a-f]{64}");

	/** Проверяет canonical lowercase SHA-256 representation. */
	public ArchiveIdentityDigest {
		Objects.requireNonNull(value, "value must not be null");
		if (!LOWERCASE_SHA_256.matcher(value).matches()) {
			throw new IllegalArgumentException(
					"value must contain exactly 64 lowercase hexadecimal characters");
		}
	}

	/**
	 * Возвращает фиксированную версию алгоритма digest.
	 *
	 * @return {@value #ALGORITHM}
	 */
	public String algorithm() {
		return ALGORITHM;
	}

	/**
	 * Создает новый одноразовый accumulator упорядоченных identities.
	 *
	 * @return пустой accumulator
	 */
	public static Accumulator accumulator() {
		return new Accumulator(newSha256());
	}

	private static MessageDigest newSha256() {
		try {
			return MessageDigest.getInstance(SHA_256);
		}
		catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException("Required SHA-256 algorithm is unavailable", exception);
		}
	}

	/**
	 * Одноразово хеширует identities в порядке добавления с четырехбайтовой
	 * big-endian длиной каждого UTF-8 значения.
	 */
	public static final class Accumulator {

		private final MessageDigest digest;
		private boolean finished;

		private Accumulator(MessageDigest digest) {
			this.digest = digest;
		}

		/**
		 * Добавляет следующую exact identity без нормализации.
		 *
		 * @param identity очередная identity
		 * @return этот accumulator
		 */
		public Accumulator addIdentity(String identity) {
			requireOpen();
			Objects.requireNonNull(identity, "identity must not be null");
			byte[] bytes = identity.getBytes(StandardCharsets.UTF_8);
			putLength(bytes.length);
			digest.update(bytes);
			return this;
		}

		/**
		 * Завершает digest и запрещает дальнейшее использование accumulator.
		 *
		 * @return immutable canonical digest
		 */
		public ArchiveIdentityDigest finish() {
			requireOpen();
			finished = true;
			return new ArchiveIdentityDigest(HexFormat.of().formatHex(digest.digest()));
		}

		private void putLength(int length) {
			for (int shift = Integer.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
				digest.update((byte) (length >>> shift));
			}
		}

		private void requireOpen() {
			if (finished) {
				throw new IllegalStateException("archive identity digest is already finished");
			}
		}
	}
}
