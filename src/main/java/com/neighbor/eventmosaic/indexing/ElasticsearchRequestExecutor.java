package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Выполняет один запрос Elasticsearch с обязательным ограничением ожидания.
 * Cycle-вариант дополнительно проверяет внешнее ownership до отправки и после
 * получения результата.
 */
interface ElasticsearchRequestExecutor {

	/**
	 * Выполняет самостоятельный запрос без cycle owner, но не дольше
	 * configured timeout Elasticsearch.
	 *
	 * @param request создание и отправка одного async-запроса
	 * @param <T> тип ответа Elasticsearch
	 * @return завершенный ответ
	 * @throws IOException при транспортном отказе
	 */
	<T> T execute(
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException;

	/**
	 * Выполняет запрос в пределах общего cycle budget и его lease.
	 *
	 * @param budget общий deadline и ownership guard текущего cycle
	 * @param request создание и отправка одного async-запроса
	 * @param <T> тип ответа Elasticsearch
	 * @return ответ, после которого ownership еще раз подтвержден
	 * @throws IOException при транспортном отказе
	 */
	<T> T execute(
			OperationBudget budget,
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException;
}
