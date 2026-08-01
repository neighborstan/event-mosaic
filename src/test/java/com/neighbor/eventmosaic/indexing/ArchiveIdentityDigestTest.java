package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Канонический digest identities архива")
class ArchiveIdentityDigestTest {

	@Test
	@DisplayName("Пустой поток совпадает с зафиксированным SHA-256 vector")
	void buildsFixedVectorForEmptyIdentityStream() {
		// Given / When
		ArchiveIdentityDigest digest = ArchiveIdentityDigest.accumulator().finish();

		// Then
		assertThat(digest.algorithm()).isEqualTo("sha256-length-prefix-v1");
		assertThat(digest.value())
				.isEqualTo("e3b0c44298fc1c149afbf4c8996fb924"
						+ "27ae41e4649b934ca495991b7852b855");
	}

	@Test
	@DisplayName("Четырехбайтовый length prefix разделяет неоднозначные последовательности")
	void buildsFixedVectorsWithBigEndianLengthPrefixes() {
		// Given / When
		ArchiveIdentityDigest first = ArchiveIdentityDigest.accumulator()
				.addIdentity("ab")
				.addIdentity("c")
				.finish();
		ArchiveIdentityDigest second = ArchiveIdentityDigest.accumulator()
				.addIdentity("a")
				.addIdentity("bc")
				.finish();

		// Then
		assertThat(first.value())
				.isEqualTo("f2939f903016e5bb29b1e4a61cdbd376"
						+ "220ca03a24180b39995f2d50f2e0a647");
		assertThat(second.value())
				.isEqualTo("b534ce16ac9c8b36823f39a395ce8e0e"
						+ "3c7ad9605b82b5444f18cadacd217a5d");
		assertThat(first).isNotEqualTo(second);
	}

	@Test
	@DisplayName("Unicode identity использует длину UTF-8 bytes")
	void buildsFixedVectorForUnicodeIdentity() {
		// Given / When
		ArchiveIdentityDigest digest = ArchiveIdentityDigest.accumulator()
				.addIdentity("событие-東京")
				.finish();

		// Then
		assertThat(digest.value())
				.isEqualTo("f24a20964fa8159d7b8d8650df7bf855"
						+ "a3c3e6fef9e68dcfc469256440ae7420");
	}

	@Test
	@DisplayName("Canonical value принимает только lowercase SHA-256 hex")
	void validatesCanonicalHexValue() {
		// Given
		String valid = "0123456789abcdef".repeat(4);

		// When / Then
		assertThat(new ArchiveIdentityDigest(valid).value()).isEqualTo(valid);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ArchiveIdentityDigest("A".repeat(64)))
				.withMessageContaining("lowercase");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ArchiveIdentityDigest("a".repeat(63)))
				.withMessageContaining("64");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ArchiveIdentityDigest("g".repeat(64)))
				.withMessageContaining("hexadecimal");
		assertThatNullPointerException()
				.isThrownBy(() -> new ArchiveIdentityDigest(null))
				.withMessage("value must not be null");
	}

	@Test
	@DisplayName("Завершенный accumulator запрещает повторное использование")
	void rejectsAccumulatorReuseAndNullIdentity() {
		// Given
		ArchiveIdentityDigest.Accumulator accumulator = ArchiveIdentityDigest.accumulator();

		// When / Then
		assertThatNullPointerException()
				.isThrownBy(() -> accumulator.addIdentity(null))
				.withMessage("identity must not be null");
		accumulator.addIdentity("event-1").finish();
		assertThatThrownBy(() -> accumulator.addIdentity("event-2"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("archive identity digest is already finished");
		assertThatThrownBy(accumulator::finish)
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("archive identity digest is already finished");
	}
}
