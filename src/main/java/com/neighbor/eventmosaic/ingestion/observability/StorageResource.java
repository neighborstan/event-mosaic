package com.neighbor.eventmosaic.ingestion.observability;

/** Ограниченный набор storage ресурсов, способных расти во время pipeline. */
public enum StorageResource {

	/** Локальный staging с GDELT ZIP и CSV. */
	STAGING,

	/** Elasticsearch data nodes с physical generations. */
	ELASTICSEARCH
}
