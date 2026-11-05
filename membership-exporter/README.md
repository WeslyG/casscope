# Cassandra membership exporter — MVP

Отдельный Java-процесс с JMX-контрактом Cassandra 4.1; практически проверен на **4.1.12**. Запускается **существующей Java Cassandra**:

```sh
/path/to/cassandra-java/bin/java -Xms16m -Xmx64m \
  -jar cassandra-membership-exporter.jar --config exporter.yaml
```

На целевой машине не нужны Docker, Maven, Python или дополнительная Java. JAR содержит Java-зависимости. Нужен обычный Java runtime с JMX/RMI и встроенным HTTP server; урезанные custom JRE отдельно не проверены.

## Реализовано

- Независимые оси: `endpoint_status{status="up|down|unknown"}` и `endpoint_ring_state{state="normal|joining|leaving|moving|unknown"}`.
- Labels `cluster`, `observer`, `endpoint` (IP **с storage port**), отдельная `observer_info{host_id}`.
- Локальные `gossip_running`, `native_transport_running`, `initialized`.
- Метрики свежести, успешности сбора, JMX-чтений, ошибок, медленных циклов и открытого circuit breaker.
- Полный набор значений status/state выдаётся как one-hot 0/1. Старые endpoint-series не сохраняются внутри collector после удаления endpoint из следующего снимка.
- Отсутствующий обязательный атрибут — ошибка сбора, а не пустой список и не Normal. Противоречивые U/D или J/L/M превращаются в unknown, увеличивается conflict counter.
- IPv4/IPv6, без reverse DNS по endpoints.

Все имена начинаются с `cassandra_membership_`. Пример:

```text
cassandra_membership_endpoint_status{cluster="cassandra-lab",observer="cassandra1",endpoint="172.18.0.4:7000",status="down"} 1
cassandra_membership_endpoint_ring_state{cluster="cassandra-lab",observer="cassandra1",endpoint="172.18.0.4:7000",state="normal"} 1
```

## Защита Cassandra от нагрузки

1. **Один target и один последовательный poller** на процесс. Новый цикл планируется только после завершения предыдущего. Нет очереди пропущенных опросов, catch-up или параллельных workers.
2. По умолчанию — пауза **30 секунд после завершения** плюс положительный jitter до 10%. Конфиг отвергает интервалы меньше 15 секунд.
3. **Один вызов `getAttributes` за цикл**: пять списков состояния и четыре локальных атрибута. Это девять getter-ов внутри Cassandra, а не один магически бесплатный getter. Стоимость списков растёт с размером кластера.
4. Никаких `queryNames`, опроса всех MBeans, token maps, ownership, table metrics, snapshot size, RMI-вызовов по каждому endpoint и управляющих операций.
5. HTTP `/metrics` читает только immutable snapshot. Частота scrape **не меняет** частоту JMX. Очередь HTTP ограничена 16 задачами, worker threads — двумя. Перегрузка HTTP может отвергать запросы, но не инициирует дополнительные JMX-вызовы.
6. Ошибка или успешное чтение медленнее 1 секунды увеличивает паузу: 60 → 120 → 240 → 300 секунд. При быстрых успешных чтениях пауза уменьшается постепенно. Немедленного повторного чтения нет даже при конфликте списков.
7. **Socket timeout открывает circuit до перезапуска exporter.** Таймаут клиента не отменяет работу на сервере. Поэтому exporter не создаёт бесконечные повторные запросы, если Cassandra уже перегружена. `/metrics` остаётся доступным, `circuit_open=1`, `next_poll_delay_seconds=0`.
8. Зависший вызов, который конкретный RMI provider не прерывает, остаётся в одном daemon worker: новый worker не создаётся. После превышения временного бюджета данные не выдаются как достоверные.
9. По умолчанию максимум 1000 endpoints в snapshot. Лимит защищает память и число метрик exporter; он не ограничивает уже сериализованный сервером JMX-ответ.

`jmx_reads_total` считает наши вызовы `getAttributes`. Подключение, закрытие, RMI protocol/GC и возможная внутренняя reconnect-логика JDK не входят в этот счётчик. Нельзя обещать ровно один сетевой пакет или один серверный метод при любой сетевой ошибке.

**Нулевого влияния гарантировать нельзя.** Тесты проверяют ограничение запросов и отсутствие fan-out; это не сертификация для перегруженного production-кластера из сотен узлов.

## Свежесть и ошибки

- До первого успешного poll — `collection_success=0`, endpoint-series отсутствуют.
- Ошибка опроса немедленно убирает endpoint-series из HTTP exposition; последний успешный timestamp сохраняется для диагностики.
- Snapshot старше `stale_after_seconds` также не публикует endpoint-series. Нет подмены отсутствующих сведений массовым DOWN.
- При большом backoff snapshot может устареть раньше следующего опроса. Это намеренный выбор в пользу снижения нагрузки.
- `/-/healthy` проверяет доступность HTTP-процесса, а не исправность Cassandra. Состояние JMX определяется метриками сбора.

## Конфиг

Пример — `exporter.yaml`. Неизвестные и повторяющиеся ключи отвергаются. YAML разбирается SafeConstructor; произвольные Java-объекты не создаются.

Для JMX с authentication указать `username_env` и `password_env`: **имена** переменных окружения, не значения паролей. Не выводим JMX URL/credentials/тексты исключений в метрики или штатные warning logs.

Timeout factory и `sun.rmi.transport.tcp.*` настраиваются только в JVM exporter. Свойства `sun.rmi` зависят от реализации JDK; Cassandra JVM не изменяется. Проверены Temurin/OpenJDK, TLS с отдельной socket factory пока не испытан.

## Что в MVP не включено

- Phi, generation и downtime: намеренно нет дополнительных вызовов.
- Token metadata / ownership / load / DC/rack соседей. Список endpoints берётся из пяти списков gossip, поэтому может быть шире строк `nodetool status`.
- Совместимость MBeans Cassandra 3.x/5.x, TLS integration tests, Windows runtime tests, scoped IPv6 с zone ID.
- Автоматический reset circuit, изменение настроек Cassandra, Ansible, dashboards и алерты.

UP в gossip не доказывает готовность CQL. Отдельная CQL-проверка остаётся нужна.

## Сборка и тесты

```sh
./mvnw clean verify
```

На Windows: `mvnw.cmd clean verify`. JDK сборочной машины может быть новее, `--release 8` ограничивает API и bytecode. Результат: `target/cassandra-membership-exporter.jar`.

В тестах:

- Реальный JMX/RMI-сервер со счётчиком операций: 200 HTTP scrape → одно batch-чтение, ноль getter/invoke по одному атрибуту.
- 200 конкурирующих попыток опроса при зависшем Reader → один вызов Reader.
- Настоящий RMI timeout в отдельной JVM → circuit открыт, HTTP отвечает, повторные batch-чтения не накапливаются.
- Backoff после медленного успешного чтения, ошибки и исчезновение старых labelsets.
- Две независимые оси состояния, пропущенные атрибуты, IPv6, cap, безопасный YAML.

Лабораторные сценарии запускаются с хоста **только на нашем стенде**:

```sh
python membership-exporter/lab_validate.py
python membership-exporter/runtime_matrix.py
```

На Windows — `py -3`. Первый сценарий кратковременно ставит node3 на паузу и гарантирует unpause в `finally`. Второй запускает временные exporter на Java 8/17/21 и удаляет только свои контейнеры. Java 11 проверяется основным экземпляром.

`lab_cql_load.py` — ограниченная read-only нагрузка для Cassandra image: 900 запросов к существующему `lab_smoke.probe`, максимум 20 запросов/с. Это не saturation benchmark.

Результаты и проверенные runtime: `../research/membership-validation/`.
