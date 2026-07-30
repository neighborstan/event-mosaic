package com.neighbor.eventmosaic;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvAccessException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvSchemaException;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltEventCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpBodyDeadline;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpStatusPolicy;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.gdelt.api.GdeltMentionCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

@DisplayName("Проверка модульной архитектуры")
class ArchitectureVerificationTest {

	@Test
	@DisplayName("Модульная структура приложения соответствует правилам")
	void verifiesApplicationModules() {
		ApplicationModules.of(EventMosaicApplication.class).verify();
	}

	@Test
	@DisplayName("GDELT публикует технические контракты через named interface api")
	void exposesGdeltContractsThroughApiNamedInterface() {
		var modules = ApplicationModules.of(EventMosaicApplication.class);
		var gdelt = modules.getModuleByName("gdelt").orElseThrow();
		var api = gdelt.getNamedInterfaces().getByName("api").orElseThrow();

		assertThat(List.of(
				GdeltArchiveKind.class,
				GdeltArchiveName.class,
				GdeltHttpBodyDeadline.class,
				GdeltHttpStatusPolicy.class,
				GdeltSourceContract.class,
				GdeltEvent.class,
				GdeltMention.class,
				GdeltCsvRecord.class,
				GdeltCsvReadSummary.class,
				GdeltCsvRecordErrorCode.class,
				GdeltCsvErrorCode.class,
				GdeltEventCsvReader.class,
				GdeltMentionCsvReader.class,
				GdeltRecordConsumer.class,
				GdeltCsvSchemaException.class,
				GdeltCsvAccessException.class,
				GdeltCsvInterruptedException.class
		)).isNotEmpty()
				.allSatisfy(type -> assertThat(api.contains(type))
				.as("%s принадлежит gdelt::api", type.getSimpleName())
				.isTrue());
	}
}
