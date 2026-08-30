package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupCommand;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupOutcome;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupResult;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationWriteOutcome;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReplaySourcePlan;
import com.neighbor.eventmosaic.ingestion.config.GenerationCleanupCommandProperties;
import com.neighbor.eventmosaic.ingestion.config.GenerationCleanupCommandProperties.Mode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.DefaultApplicationArguments;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("Аварийная серверная команда очистки поколения")
class GenerationCleanupCommandLineAdapterTest {

	private static final UUID GENERATION_UUID = UUID.fromString(
			"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
	private static final UUID OPERATION_TOKEN = UUID.fromString(
			"cccccccc-cccc-cccc-cccc-cccccccccccc");

	private final GenerationCleanupService cleanupService = mock(GenerationCleanupService.class);
	private final ObjectMapper objectMapper = JsonMapper.builder()
			.findAndAddModules()
			.build();

	@TempDir
	Path temporaryDirectory;

	@Test
	@DisplayName("Inspect сохраняет план точно выбранного поколения в новый JSON-файл")
	void inspectWritesPlanToNewJsonFile() throws Exception {
		Path planFile = temporaryDirectory.resolve("generation-cleanup-plan.json");
		GenerationCleanupPlan expected = plan();
		when(cleanupService.inspect("p20260727", GENERATION_UUID)).thenReturn(expected);

		adapter(new GenerationCleanupCommandProperties(
				Mode.INSPECT_CLEANUP,
				"p20260727",
				GENERATION_UUID,
				planFile,
				null,
				null)).run(arguments(Mode.INSPECT_CLEANUP));

		assertThat(planFile).isRegularFile();
		assertThat(objectMapper.readValue(planFile.toFile(), GenerationCleanupPlan.class))
				.isEqualTo(expected);
		verify(cleanupService).inspect("p20260727", GENERATION_UUID);
	}

	@Test
	@DisplayName("Inspect не перезаписывает существующий файл плана")
	void inspectRejectsExistingPlanFile() throws Exception {
		Path planFile = temporaryDirectory.resolve("existing-plan.json");
		Files.writeString(planFile, "protected-content");
		GenerationCleanupCommandLineAdapter adapter = adapter(
				new GenerationCleanupCommandProperties(
						Mode.INSPECT_CLEANUP,
						"p20260727",
						GENERATION_UUID,
						planFile,
						null,
						null));

		assertThatThrownBy(() -> adapter.run(arguments(Mode.INSPECT_CLEANUP)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("plan-file must not already exist");
		assertThat(Files.readString(planFile)).isEqualTo("protected-content");
		verifyNoInteractions(cleanupService);
	}

	@Test
	@DisplayName("Execute читает тот же план и передает audit-поля сервису")
	void executeReadsSavedPlanAndPassesAuditFields() throws Exception {
		Path planFile = temporaryDirectory.resolve("confirmed-plan.json");
		GenerationCleanupPlan expectedPlan = plan();
		Files.write(
				planFile,
				objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(expectedPlan));
		when(cleanupService.execute(any())).thenReturn(new GenerationCleanupResult(
				GenerationCleanupOutcome.COMPLETED,
				"p20260727",
				GENERATION_UUID,
				IndexMaintenancePhase.COMPLETED,
				OPERATION_TOKEN));

		adapter(new GenerationCleanupCommandProperties(
				Mode.CLEANUP_GENERATION,
				null,
				null,
				planFile,
				"local-operator",
				"FAILED_BUILD_CLEANUP")).run(arguments(Mode.CLEANUP_GENERATION));

		ArgumentCaptor<GenerationCleanupCommand> command =
				ArgumentCaptor.forClass(GenerationCleanupCommand.class);
		verify(cleanupService).execute(command.capture());
		assertThat(command.getValue().plan()).isEqualTo(expectedPlan);
		assertThat(command.getValue().actor()).isEqualTo("local-operator");
		assertThat(command.getValue().reasonCode()).isEqualTo("FAILED_BUILD_CLEANUP");
	}

	@Test
	@DisplayName("Незавершенная очистка возвращает ошибку процесса")
	void deferredCleanupFailsCommand() throws Exception {
		Path planFile = temporaryDirectory.resolve("deferred-plan.json");
		Files.write(
				planFile,
				objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(plan()));
		when(cleanupService.execute(any())).thenReturn(new GenerationCleanupResult(
				GenerationCleanupOutcome.MAINTENANCE_DEFERRED,
				"p20260727",
				GENERATION_UUID,
				IndexMaintenancePhase.CLEANUP_PENDING,
				OPERATION_TOKEN));
		GenerationCleanupCommandLineAdapter adapter = adapter(
				new GenerationCleanupCommandProperties(
						Mode.CLEANUP_GENERATION,
						null,
						null,
						planFile,
						"local-operator",
						"FAILED_BUILD_CLEANUP"));

		assertThatThrownBy(() -> adapter.run(arguments(Mode.CLEANUP_GENERATION)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage(
						"generation cleanup command did not complete: MAINTENANCE_DEFERRED");
	}

	private GenerationCleanupCommandLineAdapter adapter(
			GenerationCleanupCommandProperties properties
	) {
		return new GenerationCleanupCommandLineAdapter(
				cleanupService,
				properties,
				objectMapper);
	}

	private DefaultApplicationArguments arguments(Mode mode) {
		return new DefaultApplicationArguments(
				"--" + GenerationCleanupCommandLine.PROPERTY_PREFIX + ".mode=" + mode);
	}

	private GenerationCleanupPlan plan() {
		return new GenerationCleanupPlan(
				"p20260727",
				Instant.parse("2026-07-27T00:00:00Z"),
				Instant.parse("2026-08-03T00:00:00Z"),
				4,
				10,
				GENERATION_UUID,
				1,
				IndexGenerationStatus.FAILED,
				3,
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0001",
						"gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid",
				"mention-uuid",
				"FAILED_BUILD",
				GenerationWriteOutcome.NONE,
				false,
				false,
				Instant.parse("2026-08-01T11:00:00Z"),
				Instant.parse("2026-08-01T11:00:00Z"),
				null,
				"event-uuid",
				Set.of(),
				"mention-uuid",
				Set.of(),
				0,
				null,
				List.of(replaySource()),
				List.of(),
				Instant.parse("2026-08-01T12:15:00Z"),
				"f".repeat(64));
	}

	private ReplaySourcePlan replaySource() {
		return new ReplaySourcePlan(
				"archive-key",
				Instant.parse("2026-07-27T00:00:00Z"),
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				temporaryDirectory.resolve("archive.zip").toString(),
				temporaryDirectory.resolve("archive.csv").toString());
	}
}
