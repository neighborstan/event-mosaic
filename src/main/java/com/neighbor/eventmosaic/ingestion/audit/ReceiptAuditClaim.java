package com.neighbor.eventmosaic.ingestion.audit;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import java.util.UUID;

/** Сохраняет право на одну проверку и точную версию проверяемых данных. */
record ReceiptAuditClaim(
		UUID token,
		ArchiveProcessingState processing,
		ArchiveProcessingTargetBinding currentTarget
) {
}
