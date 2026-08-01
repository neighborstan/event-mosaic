package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasCutover;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import({TestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Exact Elasticsearch операции partition rebuild")
class ElasticsearchIndexMaintenanceGatewayIntegrationTest {

	private static final String OLD_EVENT = "gdelt-events-v1-p20260727-g0001";
	private static final String OLD_MENTION = "gdelt-mentions-v1-p20260727-g0001";
	private static final String NEW_EVENT = "gdelt-events-v1-p20260727-g0002";
	private static final String NEW_MENTION = "gdelt-mentions-v1-p20260727-g0002";

	@Autowired
	private IndexMaintenanceGateway gateway;

	@Autowired
	private ElasticsearchClient client;

	@BeforeEach
	void cleanIndices() throws IOException {
		client.indices().delete(request -> request
				.index(OLD_EVENT, OLD_MENTION, NEW_EVENT, NEW_MENTION)
				.ignoreUnavailable(true)
				.allowNoIndices(true));
		gateway.installTemplates();
	}

	@Test
	@DisplayName("Dedicated write block останавливает запись и снимается наблюдаемо")
	void freezesAndUnfreezesExactPairWithDedicatedApi() throws IOException {
		ExactIndexTarget event = createTarget(OLD_EVENT);
		ExactIndexTarget mention = createTarget(OLD_MENTION);

		gateway.addWriteBlock(List.of(event, mention));

		assertThat(gateway.observeExactIndex(OLD_EVENT).orElseThrow().writeBlocked())
				.isTrue();
		assertThat(gateway.observeExactIndex(OLD_MENTION).orElseThrow().writeBlocked())
				.isTrue();
		assertThatThrownBy(() -> client.index(request -> request
				.index(OLD_EVENT)
				.id("blocked-write")
				.document(Map.of("globalEventId", 1L))))
				.isInstanceOf(ElasticsearchException.class);

		gateway.removeWriteBlock(List.of(event, mention));

		assertThat(gateway.observeExactIndex(OLD_EVENT).orElseThrow().writeBlocked())
				.isFalse();
		assertThat(gateway.observeExactIndex(OLD_MENTION).orElseThrow().writeBlocked())
				.isFalse();
		assertThat(client.index(request -> request
				.index(OLD_EVENT)
				.id("allowed-write")
				.document(Map.of("globalEventId", 1L))).result().jsonValue())
				.isIn("created", "updated");
		assertThat(gateway.minimumAvailableDiskBytes()).isPositive();
	}

	@Test
	@DisplayName("Normal cutover одним request заменяет exact alias пару и сохраняет rollback block")
	void conditionallyCutsOverCompleteOldPair() {
		ExactIndexTarget oldEvent = createTarget(OLD_EVENT);
		ExactIndexTarget oldMention = createTarget(OLD_MENTION);
		ExactIndexTarget newEvent = createTarget(NEW_EVENT);
		ExactIndexTarget newMention = createTarget(NEW_MENTION);
		gateway.addStableAliases(OLD_EVENT, true, OLD_MENTION, true);
		gateway.addWriteBlock(List.of(oldEvent, oldMention));

		gateway.cutoverAliases(new AliasCutover(
				oldEvent,
				oldMention,
				newEvent,
				newMention));

		assertThat(gateway.readAliases()).satisfies(membership -> {
			assertThat(membership.eventIndices())
					.contains(NEW_EVENT)
					.doesNotContain(OLD_EVENT);
			assertThat(membership.mentionIndices())
					.contains(NEW_MENTION)
					.doesNotContain(OLD_MENTION);
		});
		assertThat(gateway.observeExactIndex(OLD_EVENT).orElseThrow().writeBlocked())
				.isTrue();
		assertThat(gateway.observeExactIndex(OLD_MENTION).orElseThrow().writeBlocked())
				.isTrue();
	}

	@Test
	@DisplayName("Repair promotion удаляет только присутствующий old membership и дополняет пару")
	void repairsMissingAndPartialAliasMembership() throws IOException {
		ExactIndexTarget oldEvent = createTarget(OLD_EVENT);
		createTarget(OLD_MENTION);
		ExactIndexTarget newEvent = createTarget(NEW_EVENT);
		ExactIndexTarget newMention = createTarget(NEW_MENTION);
		gateway.addStableAliases(OLD_EVENT, true, OLD_MENTION, false);

		gateway.cutoverAliases(new AliasCutover(
				oldEvent,
				null,
				newEvent,
				newMention));
		client.indices().updateAliases(request -> request.actions(action -> action
				.remove(remove -> remove
						.index(NEW_MENTION)
						.alias("gdelt-mentions-read")
						.mustExist(true))));

		assertThat(gateway.readAliases()).satisfies(membership -> {
			assertThat(membership.eventIndices()).contains(NEW_EVENT);
			assertThat(membership.mentionIndices()).doesNotContain(NEW_MENTION);
		});
		gateway.addStableAliases(NEW_EVENT, false, NEW_MENTION, true);

		assertThat(gateway.readAliases()).satisfies(membership -> {
			assertThat(membership.eventIndices())
					.contains(NEW_EVENT)
					.doesNotContain(OLD_EVENT);
			assertThat(membership.mentionIndices())
					.contains(NEW_MENTION)
					.doesNotContain(OLD_MENTION);
		});
	}

	private ExactIndexTarget createTarget(String indexName) {
		gateway.createExactIndex(indexName);
		var observed = gateway.observeExactIndex(indexName).orElseThrow();
		return new ExactIndexTarget(indexName, observed.indexUuid());
	}

}
