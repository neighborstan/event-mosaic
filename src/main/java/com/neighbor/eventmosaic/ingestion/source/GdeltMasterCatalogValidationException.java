package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import java.util.Objects;

/**
 * Сообщает точную причину, по которой полный каталог GDELT не прошел проверку. Более высокий уровень позже решает,
 * можно ли повторить операцию после этой ошибки.
 */
public final class GdeltMasterCatalogValidationException extends RuntimeException {

	private final IngestionErrorCode errorCode;
	private final IngestionErrorContext context;

	/** Создает ошибку каталога с указанной причиной и без дополнительных сведений. */
	public GdeltMasterCatalogValidationException(IngestionErrorCode errorCode) {
		this(errorCode, IngestionErrorContext.empty());
	}

	/** Создает ошибку каталога с указанной причиной и безопасными для журна сведениями. */
	public GdeltMasterCatalogValidationException(
			IngestionErrorCode errorCode,
			IngestionErrorContext context
	) {
		super(Objects.requireNonNull(errorCode, "errorCode must not be null").safeMessage());
		this.errorCode = errorCode;
		this.context = Objects.requireNonNull(context, "context must not be null");
	}

	/** Возвращает стабильный код причины, по которой каталог был отклонен. */
	public IngestionErrorCode errorCode() {
		return errorCode;
	}

	/** Возвращает дополнительные сведения, которые можно безопасно записать в журнал. */
	public IngestionErrorContext context() {
		return context;
	}
}
