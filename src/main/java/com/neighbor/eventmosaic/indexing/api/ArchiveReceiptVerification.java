package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Приемочная квитанция точного исходного архива.
 *
 * @param kind вид документов
 * @param expectedDocumentCount ожидаемое число документов
 * @param actualDocumentCount фактическое число документов
 * @param expectedDigest ожидаемый digest identities
 * @param actualDigest фактический digest identities
 * @param status результат сравнения count и digest
 */
public record ArchiveReceiptVerification(
		GdeltIndexKind kind,
		long expectedDocumentCount,
		long actualDocumentCount,
		ArchiveIdentityDigest expectedDigest,
		ArchiveIdentityDigest actualDigest,
		ArchiveReceiptStatus status
) {

	/**
	 * Проверяет согласованность статуса и счетчиков.
	 */
	public ArchiveReceiptVerification {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(expectedDigest, "expectedDigest must not be null");
		Objects.requireNonNull(actualDigest, "actualDigest must not be null");
		Objects.requireNonNull(status, "status must not be null");
		if (expectedDocumentCount < 0 || actualDocumentCount < 0) {
			throw new IllegalArgumentException("receipt counters must not be negative");
		}
		boolean countsEqual = expectedDocumentCount == actualDocumentCount;
		boolean digestsEqual = expectedDigest.equals(actualDigest);
		switch (status) {
			case MATCHED -> {
				if (!countsEqual || !digestsEqual) {
					throw new IllegalArgumentException(
							"matched receipt must contain equal count and digest");
				}
			}
			case SHORTAGE -> {
				if (actualDocumentCount >= expectedDocumentCount) {
					throw new IllegalArgumentException(
							"shortage receipt must contain fewer actual documents");
				}
			}
			case SURPLUS -> {
				if (actualDocumentCount <= expectedDocumentCount) {
					throw new IllegalArgumentException(
							"surplus receipt must contain more actual documents");
				}
			}
			case IDENTITY_MISMATCH -> {
				if (!countsEqual || digestsEqual) {
					throw new IllegalArgumentException(
							"identity mismatch must contain equal count and different digest");
				}
			}
		}
	}

	/** Возвращает фиксированный receipt digest algorithm. */
	public String algorithm() {
		return ArchiveIdentityDigest.ALGORITHM;
	}

	/**
	 * Возвращает признак успешной приемки архива.
	 *
	 * @return {@code true}, если count и digest совпали
	 */
	public boolean matched() {
		return status == ArchiveReceiptStatus.MATCHED;
	}

}
