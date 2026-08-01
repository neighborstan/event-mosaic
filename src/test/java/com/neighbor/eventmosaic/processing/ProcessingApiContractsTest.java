package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Контракты processing progress и result")
class ProcessingApiContractsTest {

	@Test
	@DisplayName("Progress сохраняет арифметические invariants ingestion ledger")
	void validatesProgressRelationships() {
		assertThatIllegalArgumentException()
				.isThrownBy(() ->
						new ArchiveProcessingProgress(1, 0, 2, 0, 0, 0, 0, null))
				.withMessageContaining("mappingRejectedRecords");
		assertThatIllegalArgumentException()
				.isThrownBy(() ->
						new ArchiveProcessingProgress(2, 0, 1, 2, 0, 0, 0, null))
				.withMessageContaining("submittedOperations");
		assertThatIllegalArgumentException()
				.isThrownBy(() ->
						new ArchiveProcessingProgress(1, 0, 0, 1, 0, 1, 0, null))
				.withMessageContaining("firstFailedLineNumber");
		assertThatIllegalArgumentException()
				.isThrownBy(() ->
						new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 0, 1L))
				.withMessageContaining("firstFailedLineNumber");
	}

	@Test
	@DisplayName("Completed требует полного подтверждения mapped records и receipt")
	void requiresTerminalProgressForCompletedResult() {
		ArchiveProcessingProgress completed =
				new ArchiveProcessingProgress(2, 3, 1, 1, 1, 0, 1, null);
		ArchiveProcessingProgress incompleteSubmission =
				new ArchiveProcessingProgress(2, 0, 0, 1, 1, 0, 1, null);
		ArchiveProcessingProgress incompleteReceipt =
				new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 0, null);
		ArchiveReceiptVerification receipt = matchedReceipt(1);

		ArchiveProcessingResult result = ArchiveProcessingResult.completed(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				completed,
				receipt);

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(result.receipt()).isEqualTo(receipt);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						incompleteSubmission,
						receipt))
				.withMessageContaining("COMPLETED");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						incompleteReceipt,
						receipt))
				.withMessageContaining("COMPLETED");
	}

	@Test
	@DisplayName("Result различает matched и mismatched receipt evidence")
	void validatesTerminalReceiptEvidence() {
		ArchiveProcessingProgress progress =
				new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 1, null);
		ArchiveReceiptVerification matchedReceipt = matchedReceipt(1);
		ArchiveReceiptVerification mismatchedReceipt = identityMismatchReceipt(1);
		ArchiveProcessingFailure failure = new ArchiveProcessingFailure(
				ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH,
				true,
				null);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						progress,
						mismatchedReceipt))
				.withMessageContaining("matched receipt");

		ArchiveProcessingResult failed = ArchiveProcessingResult.failed(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				progress,
				failure,
				mismatchedReceipt);

		assertThat(failed.receipt()).isEqualTo(mismatchedReceipt);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.failed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						progress,
						failure,
						matchedReceipt))
				.withMessageContaining("matched receipt");
	}

	private static ArchiveReceiptVerification matchedReceipt(long count) {
		ArchiveIdentityDigest digest = new ArchiveIdentityDigest("a".repeat(64));
		return new ArchiveReceiptVerification(
				GdeltIndexKind.EVENT,
				count,
				count,
				digest,
				digest,
				ArchiveReceiptStatus.MATCHED);
	}

	private static ArchiveReceiptVerification identityMismatchReceipt(long count) {
		return new ArchiveReceiptVerification(
				GdeltIndexKind.EVENT,
				count,
				count,
				new ArchiveIdentityDigest("a".repeat(64)),
				new ArchiveIdentityDigest("b".repeat(64)),
				ArchiveReceiptStatus.IDENTITY_MISMATCH);
	}
}
