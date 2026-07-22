package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.GdeltArchiveName;
import java.net.URI;

/**
 * Разрешает сетевой URI архива из его проверенного канонического имени.
 *
 * <p>Downloader использует этот порт вместо URI из manifest или durable state,
 * чтобы production-конфигурация оставалась единственной границей доверия.
 */
@FunctionalInterface
public interface ArchiveDownloadUriResolver {

	/**
	 * Возвращает URI объекта для проверенного имени архива.
	 *
	 * @param archiveName каноническое имя GDELT archive
	 * @return разрешенный URI объекта
	 */
	URI resolve(GdeltArchiveName archiveName);
}
