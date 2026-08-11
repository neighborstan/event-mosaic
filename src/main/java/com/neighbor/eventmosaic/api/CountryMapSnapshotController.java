package com.neighbor.eventmosaic.api;

import com.neighbor.eventmosaic.search.api.CountryMapSnapshotQuery;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Отдает один готовый снимок данных для окраски опубликованной карты стран.
 * Период выбирает сервер, поэтому endpoint не принимает параметры запроса.
 */
@RestController
@RequestMapping("/api/v1/map")
public class CountryMapSnapshotController {

	private final CountryMapSnapshotQuery snapshotQuery;

	/**
	 * Создает HTTP-границу поверх публичного поискового запроса.
	 *
	 * @param snapshotQuery чтение плотного снимка по всем регионам каталога
	 */
	public CountryMapSnapshotController(CountryMapSnapshotQuery snapshotQuery) {
		this.snapshotQuery = snapshotQuery;
	}

	/**
	 * Возвращает текущий закрытый суточный снимок без геометрии и названий стран.
	 *
	 * @param request исходный HTTP-запрос для явного запрета query string
	 * @return отдельное внешнее представление снимка
	 * @throws InvalidCountrySnapshotRequestException если в адресе есть query string
	 */
	@GetMapping("/country-snapshot")
	public CountryMapSnapshotResponse readSnapshot(HttpServletRequest request) {
		if (request.getQueryString() != null) {
			throw new InvalidCountrySnapshotRequestException();
		}
		return CountryMapSnapshotResponse.from(snapshotQuery.read());
	}
}
