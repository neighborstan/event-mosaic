package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import({TestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Безопасное удаление отдельных поисковых индексов")
class ElasticsearchIndexCleanupGatewayIntegrationTest {

	private static final String EVENT_INDEX = "gdelt-events-v1-p20260727-g0001";
	private static final String MENTION_INDEX = "gdelt-mentions-v1-p20260727-g0001";
	private static final String NON_STABLE_ALIAS = "operator-cleanup-protection";

	@Autowired
	private IndexMaintenanceGateway gateway;

	@Autowired
	private ElasticsearchClient client;

	@BeforeEach
	void cleanIndices() throws IOException {
		client.indices().delete(request -> request
				.index(EVENT_INDEX, MENTION_INDEX)
				.ignoreUnavailable(true)
				.allowNoIndices(true));
		gateway.installTemplates();
	}

	@Test
	@DisplayName("Дополнительный alias виден проверке и запрещает удаление индекса")
	void observesEveryAliasAndRejectsAliasedIndex() throws IOException {
		ExactIndexTarget event = createTarget(EVENT_INDEX);
		client.indices().updateAliases(request -> request.actions(action -> action
				.add(add -> add
						.index(EVENT_INDEX)
						.alias(NON_STABLE_ALIAS))));

		assertThat(gateway.observeAllAliasesForExactIndex(EVENT_INDEX))
				.hasValueSatisfying(observed -> {
					assertThat(observed.indexUuid()).isEqualTo(event.indexUuid());
					assertThat(observed.aliases()).containsExactly(NON_STABLE_ALIAS);
				});
		assertThatThrownBy(() -> gateway.deleteExactIndex(event))
				.isInstanceOf(IndexingProtocolException.class);
		assertThat(gateway.observeExactIndex(EVENT_INDEX)).isPresent();
	}

	@Test
	@DisplayName("Совпавший UUID без aliases разрешает удалить только выбранный индекс")
	void deletesUnaliasedIndexWithMatchingUuid() {
		ExactIndexTarget event = createTarget(EVENT_INDEX);

		gateway.deleteExactIndex(event);

		assertThat(gateway.observeExactIndex(EVENT_INDEX)).isEmpty();
		assertThat(gateway.observeAllAliasesForExactIndex(EVENT_INDEX)).isEmpty();
	}

	@Test
	@DisplayName("Частично удаленная пара оставляет доступным только сохранившийся target")
	void observesAbsentTargetAndDeletesSurvivingTarget() throws IOException {
		ExactIndexTarget event = createTarget(EVENT_INDEX);
		ExactIndexTarget mention = createTarget(MENTION_INDEX);
		client.indices().delete(request -> request.index(event.indexName()));

		assertThat(gateway.observeAllAliasesForExactIndex(EVENT_INDEX)).isEmpty();
		assertThat(gateway.observeAllAliasesForExactIndex(MENTION_INDEX))
				.hasValueSatisfying(observed ->
						assertThat(observed.indexUuid()).isEqualTo(mention.indexUuid()));

		gateway.deleteExactIndex(mention);

		assertThat(gateway.observeExactIndex(EVENT_INDEX)).isEmpty();
		assertThat(gateway.observeExactIndex(MENTION_INDEX)).isEmpty();
	}

	@Test
	@DisplayName("Новое UUID под прежним именем не удаляется по устаревшему target")
	void rejectsReplacementUuidWithoutDeletingReplacement() throws IOException {
		ExactIndexTarget stale = createTarget(EVENT_INDEX);
		client.indices().delete(request -> request.index(EVENT_INDEX));
		ExactIndexTarget replacement = createTarget(EVENT_INDEX);
		assertThat(replacement.indexUuid()).isNotEqualTo(stale.indexUuid());

		assertThatThrownBy(() -> gateway.deleteExactIndex(stale))
				.isInstanceOf(IndexingProtocolException.class);
		assertThat(gateway.observeExactIndex(EVENT_INDEX))
				.hasValueSatisfying(observed ->
						assertThat(observed.indexUuid()).isEqualTo(replacement.indexUuid()));
	}

	@Test
	@DisplayName("Размер читается только для exact UUID и не разрешает устаревший target")
	void readsStoreSizeForExactUuidOnly() {
		ExactIndexTarget event = createTarget(EVENT_INDEX);

		assertThat(gateway.exactIndexStoreBytes(event)).isGreaterThanOrEqualTo(0L);
		assertThatThrownBy(() -> gateway.exactIndexStoreBytes(new ExactIndexTarget(
				EVENT_INDEX,
				event.indexUuid() + "-stale")))
				.isInstanceOf(IndexingProtocolException.class);
	}

	private ExactIndexTarget createTarget(String indexName) {
		gateway.createExactIndex(indexName);
		var observed = gateway.observeExactIndex(indexName).orElseThrow();
		return new ExactIndexTarget(indexName, observed.indexUuid());
	}
}
