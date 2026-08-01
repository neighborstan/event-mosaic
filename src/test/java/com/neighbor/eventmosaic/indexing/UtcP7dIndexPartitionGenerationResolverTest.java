package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import java.time.Instant;
import java.time.Period;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Deterministic P7D resolver")
class UtcP7dIndexPartitionGenerationResolverTest {

	private final UtcP7dIndexPartitionGenerationResolver resolver =
			new UtcP7dIndexPartitionGenerationResolver();

	@Test
	@DisplayName("Любой UTC instant недели выбирает Monday boundary и canonical v1 names")
	void resolvesMondayUtcPartitionAndCanonicalNames() {
		var resolution = resolver.resolve(
				Instant.parse("2026-08-02T23:59:59.999Z"),
				12);

		assertThat(resolution.partition()).satisfies(partition -> {
			assertThat(partition.partitionKey()).isEqualTo("p20260727");
			assertThat(partition.startAt()).isEqualTo("2026-07-27T00:00:00Z");
			assertThat(partition.endAt()).isEqualTo("2026-08-03T00:00:00Z");
			assertThat(partition.interval()).isEqualTo(Period.ofDays(7));
		});
		assertThat(resolution.names().eventIndexName())
				.isEqualTo("gdelt-events-v1-p20260727-g0012");
		assertThat(resolution.names().mentionIndexName())
				.isEqualTo("gdelt-mentions-v1-p20260727-g0012");
	}

	@Test
	@DisplayName("Replay source update time не зависит от времени запуска")
	void replayResolvesSamePartitionAndGenerationNames() {
		Instant sourceUpdateTime = Instant.parse("2026-07-30T10:15:00Z");

		assertThat(resolver.resolve(sourceUpdateTime, 1))
				.isEqualTo(resolver.resolve(sourceUpdateTime, 1));
		assertThat(resolver.resolve(
				Instant.parse("2026-08-03T00:00:00Z"),
				1).partition().partitionKey()).isEqualTo("p20260803");
	}

	@Test
	@DisplayName("Resolver и names contract не допускают другое schema version")
	void rejectsInvalidGenerationAndSchemaVersion() {
		assertThatThrownBy(() -> resolver.resolve(Instant.EPOCH, 0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("generationNumber");
		assertThatThrownBy(() -> new IndexGenerationNames(
				"gdelt-events-v2-p20260727-g0001",
				"gdelt-mentions-v2-p20260727-g0001"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("schema v1");
		assertThatThrownBy(() -> new ActiveIndexTargets(
				"p20260727",
				2,
				1,
				UUID.randomUUID(),
				new ExactIndexTarget("gdelt-events-v1-p20260727-g0001", "event-uuid"),
				new ExactIndexTarget("gdelt-mentions-v1-p20260727-g0002", "mention-uuid")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("coherent");
	}
}
