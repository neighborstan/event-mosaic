package com.neighbor.eventmosaic.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.neighbor.eventmosaic.search.api.EventDetails;
import com.neighbor.eventmosaic.search.api.EventDetailsQuery;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(EventDetailsController.class)
@DisplayName("HTTP API деталей Event")
class EventDetailsControllerTest {

	private static final long EVENT_ID = 1_234_567_890L;

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private EventDetailsQuery eventDetailsQuery;

	@Test
	@DisplayName("Возвращает независимый details DTO для найденного события")
	void returnsEventDetails() throws Exception {
		when(eventDetailsQuery.findById(EVENT_ID)).thenReturn(
				Optional.of(eventDetails()));

		mockMvc.perform(get("/api/v1/events/{eventId}", EVENT_ID))
				.andExpect(status().isOk())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
				.andExpect(jsonPath("$.event.eventId").value(EVENT_ID))
				.andExpect(jsonPath("$.actors.actor1.code").value("USA"))
				.andExpect(jsonPath("$.classification.eventCode").value("010"))
				.andExpect(jsonPath("$.location.role").value("ACTION"))
				.andExpect(jsonPath("$.sources.length()").value(1))
				.andExpect(jsonPath("$.sources[0].identifier")
						.value("https://example.test/article"));
	}

	@Test
	@DisplayName("Возвращает 404 для отсутствующего события")
	void returnsNotFound() throws Exception {
		when(eventDetailsQuery.findById(EVENT_ID)).thenReturn(Optional.empty());

		mockMvc.perform(get("/api/v1/events/{eventId}", EVENT_ID))
				.andExpect(status().isNotFound())
				.andExpect(content().string(""));
	}

	@Test
	@DisplayName("Отклоняет неположительный идентификатор до search query")
	void rejectsNonPositiveEventId() throws Exception {
		mockMvc.perform(get("/api/v1/events/{eventId}", 0))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.code").value("INVALID_EVENT_ID"))
				.andExpect(jsonPath("$.detail")
						.value("Идентификатор события должен быть положительным"));

		verify(eventDetailsQuery, never()).findById(0);
	}

	@Test
	@DisplayName("Не раскрывает техническую причину недоступности Elasticsearch")
	void returnsSafeServiceUnavailableProblem() throws Exception {
		when(eventDetailsQuery.findById(EVENT_ID)).thenThrow(
				new SearchAccessException(new IOException("secret backend reason")));

		mockMvc.perform(get("/api/v1/events/{eventId}", EVENT_ID))
				.andExpect(status().isServiceUnavailable())
				.andExpect(jsonPath("$.code").value("SEARCH_UNAVAILABLE"))
				.andExpect(jsonPath("$.detail")
						.value("Сервис поиска временно недоступен"))
				.andExpect(content().string(
						org.hamcrest.Matchers.not(
								org.hamcrest.Matchers.containsString("secret"))));
	}

	@Test
	@DisplayName("Response DTO защищает список sources от внешних изменений")
	void keepsSourcesImmutable() {
		EventDetailsResponse mapped = EventDetailsResponse.from(eventDetails());
		var mutableSources = new ArrayList<>(mapped.sources());
		EventDetailsResponse response = new EventDetailsResponse(
				mapped.event(),
				mapped.actors(),
				mapped.classification(),
				mapped.location(),
				mutableSources);

		mutableSources.clear();

		assertThat(response.sources()).hasSize(1);
		assertThatThrownBy(response.sources()::clear)
				.isInstanceOf(UnsupportedOperationException.class);
	}

	private static EventDetails eventDetails() {
		return new EventDetails(
				new EventDetails.Event(
						EVENT_ID,
						LocalDate.of(2026, 7, 21),
						Instant.parse("2026-07-21T14:45:00Z")),
				new EventDetails.Actors(
						new EventDetails.Actor("USA", "UNITED STATES"),
						new EventDetails.Actor("RUS", "RUSSIA")),
				new EventDetails.Classification("010", "01", 1, -1.5, 2.25),
				new EventDetails.Location(
						"ACTION",
						55.7558,
						37.6173,
						"Moscow, Moskva, Russia",
						"RS",
						"-2960561"),
				List.of(new EventDetails.SourceDocument(
						"example.test",
						"https://example.test/article",
						Instant.parse("2026-07-21T14:45:00Z"),
						1.75)));
	}
}
