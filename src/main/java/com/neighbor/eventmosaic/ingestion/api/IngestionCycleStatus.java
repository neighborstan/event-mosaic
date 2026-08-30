package com.neighbor.eventmosaic.ingestion.api;

/** Показывает, свободен ли общий ingestion cycle или принадлежит одному owner. */
public enum IngestionCycleStatus {
	/** Новый trigger может попытаться получить ownership. */
	IDLE,

	/** Один trigger владеет cycle до окончания lease. */
	ACTIVE
}
