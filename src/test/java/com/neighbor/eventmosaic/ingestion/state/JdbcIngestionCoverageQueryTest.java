package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageUnavailableException;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;

@DisplayName("JDBC-запрос полноты Event-данных")
class JdbcIngestionCoverageQueryTest {

	private static final Instant FROM = Instant.parse("2026-07-20T00:00:00Z");
	private static final Instant TO = Instant.parse("2026-07-21T00:00:00Z");

	private final JdbcClient jdbcClient = mock(JdbcClient.class);
	private final JdbcIngestionCoverageQuery query = new JdbcIngestionCoverageQuery(jdbcClient);

	@Test
	@DisplayName("До обращения к базе отклоняет окно не ровно в сутки или вне 15-минутной сетки")
	void rejectsInvalidWindowBeforeDatabaseAccess() {
		assertThatThrownBy(() -> query.read(FROM, TO.minusSeconds(900)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("24 hours");
		assertThatThrownBy(() -> query.read(FROM.plusSeconds(1), TO.plusSeconds(1)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("15-minute");
		assertThatThrownBy(() -> query.read(null, TO))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("from must not be null");
		verifyNoInteractions(jdbcClient);
	}

	@Test
	@DisplayName("Ошибка доступа к данным становится безопасной повторяемой недоступностью")
	void wrapsOnlyDataAccessFailureAsTypedUnavailableOutcome() {
		DataAccessResourceFailureException failure =
				new DataAccessResourceFailureException("database unavailable");
		when(jdbcClient.sql(anyString())).thenThrow(failure);

		assertThatThrownBy(() -> query.read(FROM, TO))
				.isInstanceOfSatisfying(IngestionCoverageUnavailableException.class, exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IngestionErrorCode.COVERAGE_QUERY_UNAVAILABLE);
					assertThat(exception.getCause()).isSameAs(failure);
				});
	}

	@Test
	@DisplayName("Неожиданная ошибка программы не маскируется как неизвестная полнота")
	void doesNotWrapProgrammingDefect() {
		IllegalStateException defect = new IllegalStateException("broken test invariant");
		when(jdbcClient.sql(anyString())).thenThrow(defect);

		assertThatThrownBy(() -> query.read(FROM, TO)).isSameAs(defect);
	}
}
