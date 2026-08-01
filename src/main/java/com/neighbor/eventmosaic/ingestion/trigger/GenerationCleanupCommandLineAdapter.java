package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupCommand;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupResult;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.config.GenerationCleanupCommandProperties;
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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Дает оператору аварийную server-side команду явной очистки одной technical
 * generation. Inspect создает новый JSON-план, execute читает тот же plan и не
 * подменяет будущий защищенный асинхронный Operator API.
 */
@Component
@ConditionalOnProperty(
		prefix = GenerationCleanupCommandLine.PROPERTY_PREFIX,
		name = "mode")
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GenerationCleanupCommandLineAdapter implements ApplicationRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(
			GenerationCleanupCommandLineAdapter.class);

	private final GenerationCleanupService cleanupService;
	private final GenerationCleanupCommandProperties properties;
	private final GdeltIngestionProperties ingestionProperties;
	private final ObjectMapper objectMapper;

	/** Создает local fallback из cleanup service, настроек запуска и JSON mapper. */
	public GenerationCleanupCommandLineAdapter(
			GenerationCleanupService cleanupService,
			GenerationCleanupCommandProperties properties,
			GdeltIngestionProperties ingestionProperties,
			ObjectMapper objectMapper
	) {
		this.cleanupService = Objects.requireNonNull(
				cleanupService, "cleanupService must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.ingestionProperties = Objects.requireNonNull(
				ingestionProperties, "ingestionProperties must not be null");
		this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
	}

	@Override
	public void run(ApplicationArguments arguments) {
		Objects.requireNonNull(arguments, "arguments must not be null");
		if (!GenerationCleanupCommandLine.isRequested(arguments.getSourceArgs())) {
			throw new IllegalArgumentException(
					"generation cleanup mode must be supplied as a command-line option");
		}
		if (ingestionProperties.oneShotEnabled()) {
			throw new IllegalArgumentException(
					"generation cleanup cannot run with ingestion one-shot enabled");
		}
		if (properties.mode() == null) {
			throw new IllegalArgumentException("generation cleanup mode is required");
		}
		switch (properties.mode()) {
			case INSPECT_CLEANUP -> inspect();
			case CLEANUP_GENERATION -> cleanup();
		}
	}

	private void inspect() {
		Path planFile = requireNewPlanFile(properties.requirePlanFile());
		GenerationCleanupPlan plan = cleanupService.inspect(
				properties.requirePartitionKey(),
				properties.requireGenerationUuid());
		writePlan(planFile, plan);
		LOGGER.atInfo()
				.addKeyValue("event", "gdelt.generation_cleanup.plan_created")
				.addKeyValue("partition_key", plan.partitionKey())
				.addKeyValue("generation_uuid", plan.generationUuid())
				.addKeyValue("generation_status", plan.generationStatus())
				.addKeyValue("fingerprint", plan.fingerprint())
				.log("Generation cleanup plan created");
	}

	private void cleanup() {
		GenerationCleanupResult result = cleanupService.execute(new GenerationCleanupCommand(
				readPlan(properties.requirePlanFile()),
				properties.requireActor(),
				properties.requireReasonCode()));
		if (result.outcome() != GenerationCleanupService.GenerationCleanupOutcome.COMPLETED) {
			throw new IllegalStateException(
					"generation cleanup command did not complete: " + result.outcome());
		}
		LOGGER.atInfo()
				.addKeyValue("event", "gdelt.generation_cleanup.completed")
				.addKeyValue("partition_key", result.partitionKey())
				.addKeyValue("generation_uuid", result.generationUuid())
				.addKeyValue("phase", result.phase())
				.log("Generation cleanup command completed");
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

	private void writePlan(Path planFile, GenerationCleanupPlan plan) {
		byte[] content;
		try {
			content = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(plan);
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException(
					"Cannot serialize generation cleanup plan",
					exception);
		}
		Path temporary = null;
		try {
			temporary = Files.createTempFile(
					planFile.getParent(), ".generation-cleanup-", ".part");
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
			throw new IllegalArgumentException("Cannot create generation cleanup plan");
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
					.addKeyValue("event", "gdelt.generation_cleanup.plan_cleanup_failed")
					.log("Temporary generation cleanup plan cleanup failed");
		}
	}

	private GenerationCleanupPlan readPlan(Path planFile) {
		Path normalized = planFile.toAbsolutePath().normalize();
		if (!Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException("plan-file must be a regular file");
		}
		try (InputStream input = Files.newInputStream(normalized)) {
			return objectMapper.readValue(input, GenerationCleanupPlan.class);
		}
		catch (JacksonException | IOException exception) {
			throw new IllegalArgumentException(
					"Cannot read generation cleanup plan",
					exception);
		}
	}
}
