package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.ArchivePlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildCommand;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildOutcome;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildPlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildResult;
import com.neighbor.eventmosaic.ingestion.config.PartitionRebuildCommandProperties;
import com.neighbor.eventmosaic.ingestion.config.PartitionRebuildCommandProperties.Mode;
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

@DisplayName("Локальная команда перестроения partition")
class PartitionRebuildCommandLineAdapterTest {

	private final PartitionRebuildService rebuildService = mock(PartitionRebuildService.class);
	private final ObjectMapper objectMapper = JsonMapper.builder()
			.findAndAddModules()
			.build();
	@TempDir
	Path temporaryDirectory;

	@Test
	@DisplayName("Inspect сохраняет полный план в новый JSON-файл")
	void inspectWritesPlanToNewJsonFile() throws Exception {
		Path planFile = temporaryDirectory.resolve("partition-rebuild-plan.json");
		PartitionRebuildPlan expected = plan();
		when(rebuildService.inspect("p20260727")).thenReturn(expected);

		adapter(new PartitionRebuildCommandProperties(
				Mode.INSPECT_REBUILD,
				"p20260727",
				planFile,
				null,
				null)).run(arguments(Mode.INSPECT_REBUILD));

		assertThat(planFile).isRegularFile();
		assertThat(objectMapper.readValue(planFile.toFile(), PartitionRebuildPlan.class))
				.isEqualTo(expected);
		verify(rebuildService).inspect("p20260727");
	}

	@Test
	@DisplayName("Inspect не перезаписывает существующий файл плана")
	void inspectRejectsExistingPlanFile() throws Exception {
		Path planFile = temporaryDirectory.resolve("existing-plan.json");
		Files.writeString(planFile, "protected-content");
		PartitionRebuildCommandLineAdapter adapter = adapter(
				new PartitionRebuildCommandProperties(
						Mode.INSPECT_REBUILD,
						"p20260727",
						planFile,
						null,
						null));

		assertThatThrownBy(() -> adapter.run(arguments(Mode.INSPECT_REBUILD)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("plan-file must not already exist");
		assertThat(Files.readString(planFile)).isEqualTo("protected-content");
		verifyNoInteractions(rebuildService);
	}

	@Test
	@DisplayName("Execute читает сохраненный план и передает audit-поля сервису")
	void executeReadsSavedPlanAndPassesAuditFields() throws Exception {
		Path planFile = temporaryDirectory.resolve("confirmed-plan.json");
		PartitionRebuildPlan expectedPlan = plan();
		Files.write(
				planFile,
				objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(expectedPlan));
		PartitionRebuildResult expectedResult = new PartitionRebuildResult(
				PartitionRebuildOutcome.COMPLETED,
				"p20260727",
				IndexMaintenancePhase.COMPLETED,
				UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"));
		when(rebuildService.execute(any())).thenReturn(expectedResult);

		adapter(new PartitionRebuildCommandProperties(
				Mode.REBUILD_PARTITION,
				null,
				planFile,
				"local-operator",
				"SURPLUS_REPAIR")).run(arguments(Mode.REBUILD_PARTITION));

		ArgumentCaptor<PartitionRebuildCommand> command =
				ArgumentCaptor.forClass(PartitionRebuildCommand.class);
		verify(rebuildService).execute(command.capture());
		assertThat(command.getValue().plan()).isEqualTo(expectedPlan);
		assertThat(command.getValue().actor()).isEqualTo("local-operator");
		assertThat(command.getValue().reasonCode()).isEqualTo("SURPLUS_REPAIR");
	}

	@Test
	@DisplayName("Незавершенное восстановление возвращает ошибку процесса")
	void deferredRebuildFailsCommand() throws Exception {
		Path planFile = temporaryDirectory.resolve("deferred-plan.json");
		Files.write(
				planFile,
				objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(plan()));
		when(rebuildService.execute(any())).thenReturn(new PartitionRebuildResult(
				PartitionRebuildOutcome.MAINTENANCE_DEFERRED,
				"p20260727",
				IndexMaintenancePhase.FROZEN,
				UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc")));
		PartitionRebuildCommandLineAdapter adapter = adapter(
				new PartitionRebuildCommandProperties(
						Mode.REBUILD_PARTITION,
						null,
						planFile,
						"local-operator",
						"SURPLUS_REPAIR"));

		assertThatThrownBy(() -> adapter.run(arguments(Mode.REBUILD_PARTITION)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("partition rebuild command did not complete: MAINTENANCE_DEFERRED");
	}

	private PartitionRebuildCommandLineAdapter adapter(
			PartitionRebuildCommandProperties properties
	) {
		return new PartitionRebuildCommandLineAdapter(
				rebuildService,
				properties,
				objectMapper);
	}

	private DefaultApplicationArguments arguments(Mode mode) {
		return new DefaultApplicationArguments(
				"--" + PartitionRebuildCommandLine.PROPERTY_PREFIX + ".mode=" + mode);
	}

	private PartitionRebuildPlan plan() {
		return new PartitionRebuildPlan(
				"p20260727",
				Instant.parse("2026-07-27T00:00:00Z"),
				Instant.parse("2026-08-03T00:00:00Z"),
				IndexRepairCause.SURPLUS,
				4,
				10,
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0001",
						"gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid",
				"mention-uuid",
				"event-uuid",
				false,
				"mention-uuid",
				false,
				Set.of("gdelt-events-v1-p20260727-g0001"),
				Set.of("gdelt-mentions-v1-p20260727-g0001"),
				2,
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0002",
						"gdelt-mentions-v1-p20260727-g0002"),
				List.of(archive()),
				100,
				200,
				Instant.parse("2026-08-01T12:15:00Z"),
				"f".repeat(64));
	}

	private ArchivePlan archive() {
		return new ArchivePlan(
				"archive-key",
				Instant.parse("2026-07-27T00:00:00Z"),
				GdeltArchiveKind.TRANSLATION_EVENTS,
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				temporaryDirectory.resolve("archive.zip").toString(),
				temporaryDirectory.resolve("archive.csv").toString(),
				"b".repeat(64),
				ArchiveProcessingStatus.INDEXED,
				3,
				1,
				10,
				"c".repeat(64));
	}
}
