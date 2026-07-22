package com.neighbor.eventmosaic.ingestion.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.io.IOException;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Контракт concrete ingestion-исключений")
class IngestionExceptionContractTest {

	private static final IOException IO_CAUSE = new IOException("technical detail");

	@Test
	@DisplayName("Смысловые типы concrete и принадлежат одной shared retry-ветке")
	void semanticTypesAreConcreteAndBelongToOneSharedBranch() {
		assertThat(Modifier.isAbstract(RemoteSourceAccessException.class.getModifiers())).isFalse();
		assertThat(RemoteSourceAccessException.class.getSuperclass()).isEqualTo(RetryableException.class);
		assertThat(Modifier.isAbstract(TransferredArtifactIntegrityException.class.getModifiers())).isFalse();
		assertThat(TransferredArtifactIntegrityException.class.getSuperclass()).isEqualTo(RetryableException.class);
		assertThat(Modifier.isAbstract(RemoteResponseRejectedException.class.getModifiers())).isFalse();
		assertThat(RemoteResponseRejectedException.class.getSuperclass()).isEqualTo(NonRetryableException.class);
		assertThat(Modifier.isAbstract(SourceDataViolationException.class.getModifiers())).isFalse();
		assertThat(SourceDataViolationException.class.getSuperclass()).isEqualTo(NonRetryableException.class);
		assertThat(Modifier.isAbstract(ArchiveContentViolationException.class.getModifiers())).isFalse();
		assertThat(ArchiveContentViolationException.class.getSuperclass()).isEqualTo(NonRetryableException.class);
		assertThat(Modifier.isAbstract(StagingStorageException.class.getModifiers())).isFalse();
		assertThat(StagingStorageException.class.getSuperclass()).isEqualTo(NonRetryableException.class);
		assertThat(IngestionInterruptedException.class.getSuperclass()).isEqualTo(RetryableException.class);
	}

	@Test
	@DisplayName("Смысловой exception принимает точный code непосредственно в месте создания")
	void semanticExceptionAcceptsExactCodeAtCreationSite() {
		RemoteSourceAccessException remoteAccess = new RemoteSourceAccessException(
				IngestionErrorCode.DOWNLOAD_TIMEOUT);
		TransferredArtifactIntegrityException integrity = new TransferredArtifactIntegrityException(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
		RemoteResponseRejectedException rejected = new RemoteResponseRejectedException(
				IngestionErrorCode.DOWNLOAD_SIZE_LIMIT_EXCEEDED);
		SourceDataViolationException sourceData = new SourceDataViolationException(
				IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH);
		ArchiveContentViolationException archiveContent = new ArchiveContentViolationException(
				IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		StagingStorageException storage = new StagingStorageException(
				IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);

		assertThat(remoteAccess.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.DOWNLOAD_TIMEOUT, true));
		assertThat(integrity.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.DOWNLOAD_MD5_MISMATCH, true));
		assertThat(rejected.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.DOWNLOAD_SIZE_LIMIT_EXCEEDED, false));
		assertThat(sourceData.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH, false));
		assertThat(archiveContent.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.ZIP_CONTENT_MISMATCH, false));
		assertThat(storage.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT, false));
	}

	@Test
	@DisplayName("Общий application exception проверяет обязательный error code один раз")
	void applicationExceptionOwnsRequiredErrorCodeCheck() {
		assertThatThrownBy(() -> new TransferredArtifactIntegrityException(null))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("errorCode must not be null");
	}

	@Test
	@DisplayName("Смысловые types сохраняют только безопасный динамический контекст")
	void semanticTypesExposeOnlySafeDynamicContext() {
		SourceDataViolationException malformed = new SourceDataViolationException(
				IngestionErrorCode.MANIFEST_MALFORMED_LINE,
				IngestionErrorContext.atLine(7));
		RemoteSourceAccessException transientStatus = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR,
				IngestionErrorContext.forHttpStatus(503));
		RemoteResponseRejectedException rejectedStatus = new RemoteResponseRejectedException(
				IngestionErrorCode.DOWNLOAD_HTTP_STATUS_REJECTED,
				IngestionErrorContext.forHttpStatus(404));

		assertThat(malformed.context().lineNumber()).isEqualTo(7);
		assertThat(malformed.context().httpStatus()).isNull();
		assertThat(transientStatus.context().httpStatus()).isEqualTo(503);
		assertThat(transientStatus.context().lineNumber()).isNull();
		assertThat(rejectedStatus.context().httpStatus()).isEqualTo(404);
	}

	@Test
	@DisplayName("Смысловые types сохраняют исходную техническую причину")
	void semanticTypesPreserveTechnicalCause() {
		IOException retryableCause = new IOException("retryable detail");
		IOException nonRetryableCause = new IOException("non-retryable detail");

		assertThat(new RemoteSourceAccessException(
				IngestionErrorCode.DOWNLOAD_HTTP_ERROR,
				retryableCause))
				.hasCause(retryableCause);
		assertThat(new StagingStorageException(
				IngestionErrorCode.FILESYSTEM_IO_FAILURE,
				nonRetryableCause))
				.hasCause(nonRetryableCause);
	}

	@Test
	@DisplayName("Только interruption требует остановить весь cycle")
	void onlyInterruptionAbortsCycle() {
		IngestionInterruptedException interruption = new IngestionInterruptedException(IO_CAUSE);
		IngestionInterruptedException checkpointInterruption = new IngestionInterruptedException();

		assertThat(interruption.abortsCycle()).isTrue();
		assertThat(checkpointInterruption.failure())
				.isEqualTo(new IngestionFailure(IngestionErrorCode.OPERATION_INTERRUPTED, true));
		assertThat(checkpointInterruption).hasNoCause();
		assertThat(new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_TIMEOUT).abortsCycle()).isFalse();
		assertThat(new ArchiveContentViolationException(IngestionErrorCode.ZIP_CONTENT_MISMATCH).abortsCycle())
				.isFalse();
	}
}
