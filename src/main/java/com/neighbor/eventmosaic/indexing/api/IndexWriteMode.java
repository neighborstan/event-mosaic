package com.neighbor.eventmosaic.indexing.api;

/**
 * Определяет, пишет ли processor в текущее читаемое поколение или в теневую
 * копию, которая еще не подключена к read aliases.
 */
public enum IndexWriteMode {
	ACTIVE,
	REBUILD
}
