package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildCommand;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildPlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildResult;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.config.PartitionRebuildCommandProperties;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Дает оператору локальную команду для безопасного восстановления одной
 * partition. Сначала команда сохраняет новый JSON-план для проверки, а после
 * подтверждения читает этот же файл и запускает перестроение.
 */
@Component
@ConditionalOnProperty(
		prefix = PartitionRebuildCommandLine.PROPERTY_PREFIX,
		name = "mode")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class PartitionRebuildCommandLineAdapter implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(
			PartitionRebuildCommandLineAdapter.class);

	private final PartitionRebuildService rebuildService;
	private final PartitionRebuildCommandProperties properties;
	private final GdeltIngestionProperties ingestionProperties;
	private final ObjectMapper objectMapper;

	/**
	 * Создает локальную команду из сервиса восстановления, настроек запуска и
	 * JSON mapper.
	 */
	public PartitionRebuildCommandLineAdapter(
			PartitionRebuildService rebuildService,
			PartitionRebuildCommandProperties properties,
			GdeltIngestionProperties ingestionProperties,
			ObjectMapper objectMapper
	) {
		this.rebuildService = Objects.requireNonNull(
				rebuildService, "rebuildService must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.ingestionProperties = Objects.requireNonNull(
				ingestionProperties, "ingestionProperties must not be null");
		this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
	}

	@Override
	public void run(ApplicationArguments arguments) {
		Objects.requireNonNull(arguments, "arguments must not be null");
		if (!PartitionRebuildCommandLine.isRequested(arguments.getSourceArgs())) {
			throw new IllegalArgumentException(
					"partition rebuild mode must be supplied as a command-line option");
		}
		if (ingestionProperties.oneShotEnabled()) {
			throw new IllegalArgumentException(
					"partition rebuild cannot run with ingestion one-shot enabled");
		}
		if (properties.mode() == null) {
			throw new IllegalArgumentException("partition rebuild mode is required");
		}
		switch (properties.mode()) {
			case INSPECT_REBUILD -> inspect();
			case REBUILD_PARTITION -> rebuild();
		}
	}

	private void inspect() {
		Path planFile = requireNewPlanFile(properties.requirePlanFile());
		PartitionRebuildPlan plan = rebuildService.inspect(properties.requirePartitionKey());
		writePlan(planFile, plan);
		LOGGER.atInfo()
				.addKeyValue("event", "gdelt.partition_rebuild.plan_created")
				.addKeyValue("partition_key", plan.partitionKey())
				.addKeyValue("repair_cause", plan.repairCause())
				.addKeyValue("fingerprint", plan.fingerprint())
				.log("Partition rebuild plan created");
	}

	private void rebuild() {
		PartitionRebuildResult result = rebuildService.execute(new PartitionRebuildCommand(
				readPlan(properties.requirePlanFile()),
				properties.requireActor(),
				properties.requireReasonCode()));
		if (result.outcome() != PartitionRebuildService.PartitionRebuildOutcome.COMPLETED) {
			throw new IllegalStateException(
					"partition rebuild command did not complete: " + result.outcome());
		}
		LOGGER.atInfo()
				.addKeyValue("event", "gdelt.partition_rebuild.completed")
				.addKeyValue("partition_key", result.partitionKey())
				.addKeyValue("outcome", result.outcome())
				.addKeyValue("phase", result.phase())
				.log("Partition rebuild command completed");
	}

	private static Path requireNewPlanFile(Path planFile) {
		Path normalized = planFile.toAbsolutePath().normalize();
		Path parent = normalized.getParent();
		if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException(
					"plan-file parent must be an existing real directory");
		}
		if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException("plan-file must not already exist");
		}
		return normalized;
	}

	private void writePlan(Path planFile, PartitionRebuildPlan plan) {
		byte[] content;
		try {
			content = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(plan);
		}
		catch (RuntimeException exception) {
			throw new IllegalArgumentException("Cannot serialize partition rebuild plan");
		}
		Path temporary = null;
		try {
			temporary = Files.createTempFile(
					planFile.getParent(), ".partition-rebuild-", ".part");
			Files.write(
					temporary,
					content,
					StandardOpenOption.TRUNCATE_EXISTING,
					StandardOpenOption.WRITE,
					LinkOption.NOFOLLOW_LINKS);
			Files.move(temporary, planFile);
		}
		catch (IOException exception) {
			deleteTemporaryPlan(temporary);
			throw new IllegalArgumentException("Cannot create partition rebuild plan");
		}
	}

	private static void deleteTemporaryPlan(Path temporary) {
		if (temporary == null) {
			return;
		}
		try {
			Files.deleteIfExists(temporary);
		}
		catch (IOException exception) {
			LOGGER.atWarn()
					.addKeyValue("event", "gdelt.partition_rebuild.plan_cleanup_failed")
					.log("Temporary partition rebuild plan cleanup failed");
		}
	}

	private PartitionRebuildPlan readPlan(Path planFile) {
		Path normalized = planFile.toAbsolutePath().normalize();
		if (!Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException("plan-file must be a regular file");
		}
		try (InputStream input = Files.newInputStream(normalized)) {
			return objectMapper.readValue(input, PartitionRebuildPlan.class);
		}
		catch (IOException exception) {
			throw new IllegalArgumentException("Cannot read partition rebuild plan");
		}
	}
}
