package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlanner;
import com.neighbor.eventmosaic.shared.time.RollingWindowPolicy;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

public final class GdeltTestFixtures {

	public static final Instant UPDATE_TIME = Instant.parse("2026-07-20T12:00:00Z");
	public static final String EVENT_MD5 = "0123456789abcdef0123456789abcdef";
	public static final String MENTION_MD5 = "fedcba9876543210fedcba9876543210";

	private GdeltTestFixtures() {
	}

	public static DiscoveredUpdate update(Instant updateTime) {
		return update(updateTime, EVENT_MD5);
	}

	public static DiscoveredUpdate update(Instant updateTime, String eventMd5) {
		return new DiscoveredUpdate(
				updateTime,
				List.of(
						archive(updateTime, ArchiveType.TRANSLATION_EVENTS, eventMd5, 1024),
						archive(updateTime, ArchiveType.TRANSLATION_MENTIONS, MENTION_MD5, 1024)
				),
				List.of()
		);
	}

	public static DiscoveredArchive archive(
			Instant updateTime,
			ArchiveType type,
			String md5,
			long expectedSizeBytes
	) {
		String name = archiveName(updateTime, type);
		return new DiscoveredArchive(
				updateTime,
				name,
				URI.create("http://data.gdeltproject.org/gdeltv2/" + name),
				md5,
				type,
				expectedSizeBytes
		);
	}

	public static String archiveName(Instant updateTime, ArchiveType type) {
		String kind = switch (type) {
			case TRANSLATION_EVENTS -> "export";
			case TRANSLATION_MENTIONS -> "mentions";
		};
		return GdeltSourceContract.formatUpdateTimestamp(updateTime)
				+ ".translation." + kind + ".CSV.zip";
	}

	public static GdeltIngestionProperties properties(Path stagingRoot, long maxArchiveBytes) {
		return properties(stagingRoot, maxArchiveBytes, FirstRunPolicy.LATEST);
	}

	public static GdeltIngestionProperties properties(
			Path stagingRoot,
			long maxArchiveBytes,
			FirstRunPolicy firstRunPolicy
	) {
		return new GdeltIngestionProperties(
				GdeltSourceContract.OFFICIAL_DOWNLOAD_BASE_URI,
				stagingRoot,
				new GdeltIngestionProperties.Http(
						65536,
						maxArchiveBytes,
						Duration.ofSeconds(1),
						Duration.ofSeconds(5)),
				new GdeltIngestionProperties.Zip(4, 1024 * 1024, 1024 * 1024),
				new GdeltIngestionProperties.Continuity(
						Duration.ofMinutes(15),
						firstRunPolicy,
						null),
				new GdeltIngestionProperties.Automatic(
						false,
						Duration.ofMinutes(1),
						Duration.ofMinutes(15),
						Duration.ofSeconds(30),
						Duration.ofMinutes(5),
						Duration.ofMinutes(30),
						256,
						new GdeltIngestionProperties.ReceiptAudit(
								Duration.ofMinutes(15),
								2)),
				false
		);
	}

	public static RecentWindowPlanner recentWindowPlanner() {
		return new RecentWindowPlanner(
				Clock.fixed(UPDATE_TIME.plus(Duration.ofMinutes(30)), ZoneOffset.UTC),
				new RollingWindowPolicy(
						GdeltSourceContract.UPDATE_INTERVAL,
						Duration.ofHours(24),
						Duration.ofMinutes(15)));
	}

	public static BackendDataProperties backendDataProperties() {
		return new BackendDataProperties(
				Duration.ofDays(7),
				new BackendDataProperties.Retry(
						Duration.ofMinutes(1),
						2.0,
						Duration.ofMinutes(15),
						0.1,
						3),
				Duration.ofMinutes(12),
				500,
				new BackendDataProperties.DiskPressure(1, 1),
				new BackendDataProperties.Rebuild(
						Duration.ofMinutes(15),
						Duration.ofMinutes(15)),
				new BackendDataProperties.Cleanup(
						Duration.ofHours(24),
						Duration.ofDays(7),
						Duration.ofMinutes(15),
						Duration.ofMinutes(15),
						false));
	}
}
