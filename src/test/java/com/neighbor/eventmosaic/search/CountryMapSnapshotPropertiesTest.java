package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Настройки серверного снимка по странам")
class CountryMapSnapshotPropertiesTest {

	@Test
	@DisplayName("Принимает безопасную версию и запас от нуля до четырех интервалов")
	void acceptsSafeVersionAndBoundedGrace() {
		assertThatCode(() -> properties(Duration.ZERO)).doesNotThrowAnyException();
		assertThatCode(() -> properties(Duration.ofMinutes(15))).doesNotThrowAnyException();
		assertThatCode(() -> properties(Duration.ofHours(1))).doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Отклоняет версию, которая не является точным безопасным адресом каталога")
	void rejectsUnsafeGeometryVersion() {
		assertThatThrownBy(() -> new CountryMapSnapshotProperties(
				"../country-v1",
				Duration.ofMinutes(15)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("country-vN");
		assertThatThrownBy(() -> new CountryMapSnapshotProperties(
				"country-v0",
				Duration.ofMinutes(15)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("country-vN");
	}

	@Test
	@DisplayName("Отклоняет отрицательный запас времени")
	void rejectsNegativeGrace() {
		assertThatThrownBy(() -> properties(Duration.ofMinutes(-15)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("must not be negative");
	}

	@Test
	@DisplayName("Отклоняет запас с долей секунды или неполным интервалом")
	void rejectsGraceOutsideWholeCadenceSteps() {
		assertThatThrownBy(() -> properties(
				Duration.ofMinutes(15).plusNanos(1)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("whole seconds");
		assertThatThrownBy(() -> properties(Duration.ofMinutes(10)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("multiple of 15 minutes");
	}

	@Test
	@DisplayName("Отклоняет запас больше четырех исходных интервалов")
	void rejectsGraceAboveFourCadences() {
		assertThatThrownBy(() -> properties(Duration.ofMinutes(75)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("must not exceed one hour");
	}

	private static CountryMapSnapshotProperties properties(Duration ingestionGrace) {
		return new CountryMapSnapshotProperties("country-v1", ingestionGrace);
	}
}
