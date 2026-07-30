package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
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

		ArchiveProcessingResult result = ArchiveProcessingResult.completed(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				completed);

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						incompleteSubmission))
				.withMessageContaining("COMPLETED");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						incompleteReceipt))
				.withMessageContaining("COMPLETED");
	}
}
