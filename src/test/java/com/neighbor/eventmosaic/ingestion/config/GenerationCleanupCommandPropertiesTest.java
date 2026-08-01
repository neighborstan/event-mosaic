package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.ingestion.config.GenerationCleanupCommandProperties.Mode;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Параметры серверной команды очистки поколения")
class GenerationCleanupCommandPropertiesTest {

	private static final UUID GENERATION_UUID =
			UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
	private static final Path PLAN_FILE = Path.of("generation-cleanup-plan.json");

	@Test
	@DisplayName("Возвращает проверенные идентификаторы, файл плана и данные оператора")
	void returnsValidatedCommandValues() {
		GenerationCleanupCommandProperties properties = properties(
				"p20260727",
				GENERATION_UUID,
				PLAN_FILE,
				"local-operator",
				"SUPERSEDED_CLEANUP");

		assertThat(properties.mode()).isEqualTo(Mode.INSPECT_CLEANUP);
		assertThat(properties.requirePartitionKey()).isEqualTo("p20260727");
		assertThat(properties.requireGenerationUuid()).isEqualTo(GENERATION_UUID);
		assertThat(properties.requirePlanFile()).isEqualTo(PLAN_FILE);
		assertThat(properties.requireActor()).isEqualTo("local-operator");
		assertThat(properties.requireReasonCode()).isEqualTo("SUPERSEDED_CLEANUP");
	}

	@Test
	@DisplayName("Отклоняет отсутствующие или неточные идентификаторы и файл плана")
	void rejectsMissingOrInvalidInspectValues() {
		assertThatThrownBy(() -> properties(
				"20260727",
				GENERATION_UUID,
				PLAN_FILE,
				null,
				null).requirePartitionKey())
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("partition-key");
		assertThatThrownBy(() -> properties(
				"p20260727",
				null,
				PLAN_FILE,
				null,
				null).requireGenerationUuid())
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("generation-uuid");
		assertThatThrownBy(() -> properties(
				"p20260727",
				GENERATION_UUID,
				null,
				null,
				null).requirePlanFile())
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("plan-file");
	}

	@Test
	@DisplayName("Отклоняет небезопасные данные оператора для выполнения очистки")
	void rejectsInvalidExecuteAuditValues() {
		assertThatThrownBy(() -> properties(
				"p20260727",
				GENERATION_UUID,
				PLAN_FILE,
				"local operator",
				"SUPERSEDED_CLEANUP").requireActor())
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("actor");
		assertThatThrownBy(() -> properties(
				"p20260727",
				GENERATION_UUID,
				PLAN_FILE,
				"local-operator",
				"cleanup now").requireReasonCode())
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("reason-code");
	}

	private static GenerationCleanupCommandProperties properties(
			String partitionKey,
			UUID generationUuid,
			Path planFile,
			String actor,
			String reasonCode
	) {
		return new GenerationCleanupCommandProperties(
				Mode.INSPECT_CLEANUP,
				partitionKey,
				generationUuid,
				planFile,
				actor,
				reasonCode);
	}
}
