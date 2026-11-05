# Конфигурация Casscope

Конфигурация — плоский YAML mapping, путь задаётся через `--config`. Пример: [exporter.yaml](../examples/exporter.yaml).

Неизвестные/повторяющиеся ключи, некорректные числовые ограничения и файл больше 64 KiB приводят к отказу запуска. SafeConstructor не создаёт Java-объекты из YAML. Изменения читаются при старте; после редактирования перезапустите Casscope.

## Параметры

| Параметр | Default в коде | Ограничения и смысл |
|---|---|---|
| `cluster` | `cassandra` | 1–128 символов; постоянная метка кластера |
| `observer` | `local` | 1–128 символов; стабильное уникальное имя наблюдателя внутри кластера |
| `jmx_url` | `service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi` | URL начинается с `service:jmx:rmi:`; подключение к одной Cassandra |
| `listen_address` | `127.0.0.1` | Адрес HTTP; `0.0.0.0` открывает все IPv4-интерфейсы |
| `port` | `9505` | 1–65535; HTTP port |
| `poll_interval_seconds` | `30` | 15–3600; пауза после завершения цикла, затем положительный jitter |
| `max_backoff_seconds` | `300` | Не меньше poll interval, не больше 3600; верхняя граница паузы с jitter |
| `timeout_retry_seconds` | `60` | Не меньше 60 и poll interval; минимальная пауза перед первой пробой после socket timeout |
| `max_timeout_backoff_seconds` | `900` | Не меньше timeout_retry_seconds, не больше 3600; максимум timeout cooldown вместе с jitter |
| `socket_timeout_ms` | `3000` | 100–30000; socket connect/read/handshake настройки, не универсальный deadline всего poll |
| `slow_poll_ms` | `1000` | 1…socket timeout; длительность успешного poll, с которой он считается медленным |
| `stale_after_seconds` | `90` | poll interval…7200; максимальный возраст публикуемого snapshot |
| `max_endpoints` | `1000` | 1–10000; превышение вызывает ошибку сбора, а не усечение списка |
| `username_env` | Не задан | Имя переменной окружения с JMX username |
| `password_env` | Не задан | Имя переменной окружения с JMX password; задаётся вместе с username_env |

Примеры могут задавать другие cluster/observer. Default — не автоматическое обнаружение имени узла: задавайте observer явно.

## JMX authentication и transport

```yaml
username_env: CASSCOPE_JMX_USER
password_env: CASSCOPE_JMX_PASSWORD
```

В окружении процесса должны существовать обе переменные со значениями. Их имена, а не пароли, помещаются в YAML. Доступ к JMX ограничивается настройками Cassandra; её JVM Casscope не изменяет.

При удалённом JMX учитывайте registry port, RMI server port и advertised hostname. Доступность только registry ещё не означает работоспособное соединение. Не открывайте JMX наружу без соответствующих authentication/TLS и сетевых ограничений.

SSL JMX использует механизмы Java/RMI и настройки truststore JVM. Полная TLS-матрица пока не проверена; отдельная socket factory сервера может иначе обрабатывать timeout. Свойства `sun.rmi.transport.tcp.*` зависят от JDK implementation и действуют только внутри standalone-процесса Casscope.

## Опрос и свежесть

- Один цикл — один application `getAttributes` к `org.apache.cassandra.db:type=StorageService`.
- Читаются `LiveNodesWithPort`, `UnreachableNodesWithPort`, `JoiningNodesWithPort`, `LeavingNodesWithPort`, `MovingNodesWithPort`, `GossipRunning`, `NativeTransportRunning`, `Initialized`, `LocalHostId`.
- Нет запросов на каждый endpoint и сканирования всех MBeans. Чтение списков всё равно имеет стоимость, зависящую от размера кластера.
- Неудачный или медленный цикл увеличивает задержку вдвое до max backoff. Быстрый успешный цикл уменьшает её постепенно до poll interval.
- Socket timeout открывает circuit. Через timeout_retry_seconds начинается одна проба half_open; следующая неудача увеличивает паузу вдвое до max_timeout_backoff_seconds. Между пробами нет обращений JMX. Успешный полный снимок закрывает circuit автоматически; restart не нужен. Ошибка другого типа вне circuit приводит к обычному backoff/reconnect.
- Для быстрых успешных чтений задержка ограничивается также бюджетом свежести snapshot. Медленные чтения продолжают увеличивать backoff. Jitter не сокращает минимальную паузу.
- Новый цикл начинается после возврата предыдущего, включая закрытие соединения. Timeout не гарантирует отмену серверной работы: автоматические пробы ограничивают её возможное поступление, но не дают абсолютной гарантии отсутствия оставшихся серверных операций.
- При зависшем worker не создаётся второй worker. Возраст данных и текущий in-flight бюджет учитываются при решении о свежести.
- Цикл дольше `2 × socket_timeout_ms` считается stalled: HTTP readiness уже неуспешна. Если такой цикл позже вернул данные, результат отбрасывается и запускается timeout backoff; поздний ответ не становится новым «свежим» snapshot.
- Сбор не является атомарной транзакцией Cassandra. Противоречивые списки дают unknown/conflict, без немедленного повторного запроса.

HTTP использует два worker threads и очередь на 16 задач. При перегрузке запросы могут отвергаться; это не запускает JMX. Процесс, heap, native memory и Cassandra делят ресурсы машины, поэтому частые scrape не являются бесплатными даже при кэшировании.

## Метрики

У всех метрик prefix `cassandra_membership_` и labels `cluster`, `observer`.

| Имя после prefix | Тип | Значение |
|---|---|---|
| `endpoint_status` | gauge | По `endpoint,status`; one-hot up/down/unknown |
| `endpoint_ring_state` | gauge | По `endpoint,state`; one-hot normal/joining/leaving/moving/unknown |
| `observer_info` | gauge | Значение 1 и label `host_id` наблюдателя |
| `gossip_running` | gauge | Локальный gossip включён, 0/1 |
| `native_transport_running` | gauge | Локальный CQL transport включён, 0/1 |
| `initialized` | gauge | Локальный признак инициализации, 0/1 |
| `endpoints` | gauge | Размер объединения пяти списков membership |
| `collection_success` | gauge | Достоверный свежий snapshot доступен, 0/1 |
| `snapshot_timestamp_seconds` | gauge | Unix time последнего успешного snapshot; до первого успеха 0 |
| `snapshot_age_seconds` | gauge | Возраст snapshot по monotonic clock; до первого успеха -1 |
| `poll_in_flight` | gauge | Цикл опроса выполняется, 0/1 |
| `circuit_open` | gauge | После timeout активен режим редких автоматических проб, включая half_open, 0/1 |
| `polls_total` | counter | Начатые application poll cycles |
| `jmx_reads_total` | counter | Попытки application getAttributes после подключения; доставка и выполнение на сервере не гарантированы. Не включает весь RMI control traffic |
| `collection_errors_total` | counter | Неудачные циклы |
| `slow_polls_total` | counter | Успешные, но медленные циклы |
| `state_conflicts_total` | counter | Наблюдавшиеся конфликты по двум осям, а не число уникальных инцидентов |
| `last_poll_duration_seconds` | gauge | Длительность завершившегося цикла |
| `next_poll_delay_seconds` | gauge | Плановая задержка после завершения последнего цикла |
| `retry_after_seconds` | gauge | Оставшееся время до следующей попытки; 0 во время выполнения |
| `consecutive_failures` | gauge | Число подряд завершившихся ошибкой циклов; сбрасывается успешным снимком |
| `timeouts_total` | counter | Циклы с socket timeout |
| `recovery_attempts_total` | counter | Автоматические пробы timeout recovery |
| `recoveries_total` | counter | Успешные снимки, закрывшие timeout circuit |
| `collector_state` | gauge | One-hot по state: starting/healthy/backoff/open/half_open/stale/stalled/stopped |

Endpoint — литерал IP со storage port, IPv6 в скобках. Scoped IPv6 с zone ID не поддерживается. Список gossip может содержать записи, не являющиеся действующими владельцами токенов: это не точная копия состава token ring.

## HTTP и сигнализация отказа

| Path | Успешный свежий JMX snapshot | Нет свежего успешного snapshot |
|---|---|---|
| `/metrics` | 200, membership и диагностика; collection_success=1 | **200**, только диагностика; collection_success=0 |
| `/diagnostics/metrics` | 200, только health/counters | 200, только health/counters; collection_success=0 |
| `/-/ready` | 200 | 503 |
| `/-/healthy` | 200, процесс жив | 200, процесс жив |

Collection_success и membership exposition используют один captured view; scrape не инициирует JMX. Старт до первого снимка, ошибка любого poll, истечение свежести и зависший worker дают collection_success=0. При этом не публикуются endpoint_status, endpoint_ring_state, gossip_running, native_transport_running, initialized, endpoints и observer_info из прежнего снимка. Счётчики, collector_state, snapshot_age_seconds и retry_after_seconds продолжают публиковаться в том же /metrics. Snapshot_timestamp_seconds — время последнего успешного снимка, даже если он уже недостоверен; до первого успеха 0.

Prometheus up=1 означает успешный HTTP-scrape, а collection_success=1 — наличие достоверного свежего снимка Cassandra. Недоступный процесс или некорректный/неуспешный HTTP-scrape дают up=0. Свежий снимок может сообщать DOWN, выключенный gossip или CQL: это корректный сбор, HTTP 200 и collection_success=1.

Для уведомлений подключите [пример alerts](../examples/prometheus/casscope-alerts.yml): отказ наблюдаемости определяется по up=0 **или** collection_success=0; алерты состояния Cassandra требуют up=1 и collection_success=1. Основной job читает /metrics; второго job для диагностики не требуется. /diagnostics/metrics предоставляет только диагностическую часть для ручного просмотра. /-/ready сохраняет HTTP 503 при отсутствии достоверного снимка.

Не превращайте исчезнувшие peer-series в UP или массовый DOWN: при отказе сбора состояние неизвестно. После изменения состава старые endpoint-series исчезают из нового snapshot.

Phi, generation, downtime, ownership, load и metadata соседей в эту версию не входят.
