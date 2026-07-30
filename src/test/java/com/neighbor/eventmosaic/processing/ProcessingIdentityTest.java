package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Стабильные идентификаторы обработки")
class ProcessingIdentityTest {

	private final MentionIdentityFactory mentionIdentityFactory = new MentionIdentityFactory();
	private final DefaultProcessingFingerprintFactory fingerprintFactory =
			new DefaultProcessingFingerprintFactory();

	@Test
	@DisplayName("Строит raw Mention ID по зафиксированному versioned vector")
	void buildsRawMentionIdFromVersionedVector() {
		String first = mentionIdentityFactory.rawMentionId(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				42);
		String replay = mentionIdentityFactory.rawMentionId(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				42);

		assertThat(first)
				.isEqualTo(
						"rm1-21390426c0e9d66c0d2415422fc3806e4e041afc5713a10be7a7199d58015ecf")
				.isEqualTo(replay);
		assertThat(mentionIdentityFactory.rawMentionId(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				43)).isNotEqualTo(first);
	}

	@Test
	@DisplayName("Сохраняет exact UTF-8 identifier при построении source document key")
	void preservesExactIdentifierInSourceDocumentKey() {
		String identifier = "  https://пример.рф/a  ";

		String key = mentionIdentityFactory.sourceDocumentKey(700_000_001L, 1, identifier);

		assertThat(key)
				.isEqualTo(
						"sd1-3dfe580f6f02b0edb263b53ad4196cdc0baf6642971e2d2d65372690907c2b49");
		assertThat(mentionIdentityFactory.sourceDocumentKey(
				700_000_001L,
				1,
				identifier.trim())).isNotEqualTo(key);
		assertThat(mentionIdentityFactory.sourceDocumentKey(
				700_000_001L,
				2,
				identifier)).isNotEqualTo(key);
	}

	@Test
	@DisplayName("Length prefix различает неоднозначные конкатенации строк")
	void framesVariableLengthStrings() {
		String left = FramedSha256.create()
				.putString("ab")
				.putString("c")
				.finishHex();
		String right = FramedSha256.create()
				.putString("a")
				.putString("bc")
				.finishHex();

		assertThat(left).isNotEqualTo(right);
	}

	@Test
	@DisplayName("Строит processing fingerprint как 64 lowercase hex")
	void buildsProcessingFingerprintFromAllRevisionInputs() {
		String fingerprint = fingerprintFactory.create(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				GdeltArchiveKind.TRANSLATION_MENTIONS,
				"projection-v1",
				"mapping-v1");

		assertThat(fingerprint)
				.isEqualTo("d0023152f7715b3f6099c4cef579808e46e3a0212dfa4b9698d8076ff8a99c83")
				.matches("[0-9a-f]{64}");
		assertThat(fingerprintFactory.create(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				GdeltArchiveKind.TRANSLATION_MENTIONS,
				"projection-v2",
				"mapping-v1")).isNotEqualTo(fingerprint);
		assertThat(fingerprintFactory.create(
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				GdeltArchiveKind.TRANSLATION_EVENTS,
				"projection-v1",
				"mapping-v1")).isNotEqualTo(fingerprint);
	}

	@Test
	@DisplayName("Одноразовый framed hash запрещает запись после завершения")
	void rejectsDigestReuse() {
		FramedSha256 digest = FramedSha256.create().putString("value");

		digest.finishHex();

		assertThatThrownBy(() -> digest.putLong(1))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("framed digest is already finished");
	}
}
