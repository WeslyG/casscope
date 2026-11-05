# План реализации JMX-exporter состояния Cassandra

Дата: 5 ноября 2026. Целевая версия первого выпуска: Apache Cassandra **4.1.12**.

Реализован первый MVP в `membership-exporter/`. Этот документ описывает полный замысел; точный текущий scope и ограничения — в `membership-exporter/README.md`, результаты — в `research/membership-validation/`.

**Приоритет защиты Cassandra:** в реализованном MVP интервал по умолчанию увеличен до 30 секунд, минимальный разрешённый — 15 секунд. Один batch-чтение за цикл, без token maps и per-endpoint RPC. При медленном чтении/ошибках используется backoff до 300 секунд. После socket timeout опрос приостанавливается до перезапуска exporter. Ранние примеры интервалов ниже — исходный дизайн, а не действующие defaults.

**Уточнение области:** сейчас разрабатываем сам exporter и лабораторные тесты. Ansible-роли, production-развёртывание, алерты, dashboards и эксплуатационное сопровождение исключены из текущей работы.

**Обязательное ограничение совместимости:** на целевой машине используем тот же установленный Java runtime, что и Cassandra. Не требуем ни второй Java, ни Docker. Это отдельный Java-процесс, а не внедрение кода в JVM Cassandra.

## 1. Цель и границы

Дополнить существующий DataStax MCAC метриками, позволяющими ответить:

- Считает ли узел A узел B доступным?
- В каком состоянии кольца A видит B: Normal, Joining, Leaving или Moving?
- Совпадают ли представления разных наблюдателей?
- Насколько свежие эти сведения и удалось ли вообще прочитать JMX?
- Работают ли gossip и native transport локального наблюдаемого процесса?

Не заменять MCAC, не переносить сбор latency/compactions/table metrics, не встраивать автоматическое восстановление. Никаких `repair`, `decommission`, `assassinate`, изменения phi threshold и других управляющих действий внутри exporter.

Отдельно сохранить CQL-probe. Даже точное совпадение с `nodetool status` не доказывает доступность CQL, успешную запись с нужным consistency level или исправность данных.

## 2. Технический стек и архитектура

### Зафиксированный выбор технологий

| Часть | Выбор | Обоснование |
|---|---|---|
| Язык и runtime | **Java 8 bytecode**, standalone-процесс на уже установленной Java Cassandra | JMX/RMI есть в стандартной Java; обычные immutable-классы и concurrency Java 8 |
| JMX | `javax.management` и `javax.management.remote` из JDK | Прямое подключение к Cassandra, без Jolokia и без Cassandra JAR |
| Сборка | **Maven Wrapper**, `maven.compiler.release=8` | Одна воспроизводимая сборка и единый способ запуска тестов |
| Prometheus | Официальный **client_java 1.9.0** | Готовые типы метрик и корректная HTTP exposition вместо собственного текстового формата |
| HTTP | `prometheus-metrics-exporter-httpserver` | Небольшой встроенный HTTP-сервер; отдельный web framework не нужен |
| Конфиг | YAML через **SnakeYAML 2.x** (`org.yaml:snakeyaml`), с SafeConstructor | Разбор в обычные структуры данных и явная проверка схемы; без конструирования произвольных Java-объектов |
| Планировщик | `ScheduledExecutorService` | Один последовательный poller на JMX target, без наложения опросов |
| Состояние | Immutable POJO Java 8 + `AtomicReference` | HTTP всегда читает один целостный опубликованный snapshot |
| Логи | `java.util.logging` | Достаточно для небольшого exporter; отдельный logging framework в MVP не нужен |
| Unit tests | **JUnit 5 + AssertJ** | Проверка нормализации, переходов состояния, ошибок и контрактов метрик |
| JMX integration tests | Локальный тестовый `MBeanServer` и `JMXConnectorServer` | Проверяем реальные JMX/RMI типы и вызовы, а не только mocks |
| Проверка на Cassandra | Наш Docker-стенд 4.1.12, сценарии на Python | Реальные pause/recovery/CQL/JMX failures; Docker здесь используется как лаборатория |
| Артефакт | Один исполняемый **JAR**, собранный Maven Shade Plugin | Запуск `java -jar`; shaded dependencies без установки дополнительного сервера |

Версия Prometheus взята из текущей официальной документации client_java. Java 8 baseline upstream проверен по pom.xml; все реально включённые транзитивные зависимости и итоговый JAR дополнительно проверяются на каждой целевой JVM. Все зависимости и Maven plugins закрепляем точными версиями в первом `pom.xml`; `2.x` и `JUnit 5` здесь обозначают выбранные семейства, а не плавающие зависимости будущей сборки. Не переносить автоматически версии из старых exporter.

Зависимости Prometheus:

```xml
<!-- Все модули одной закреплённой версии через prometheus-metrics-bom:1.9.0 -->
<dependency>
  <groupId>io.prometheus</groupId>
  <artifactId>prometheus-metrics-core</artifactId>
</dependency>
<dependency>
  <groupId>io.prometheus</groupId>
  <artifactId>prometheus-metrics-exporter-httpserver</artifactId>
</dependency>
```

`prometheus-metrics-instrumentation-jvm` для метрик JVM самого exporter можно добавить позднее, если понадобится в лабораторных измерениях. Это не источник метрик Cassandra.

Не добавляем Spring Boot, Quarkus, WebFlux, ORM, Kafka, БД или Redis. Задача — периодически прочитать несколько MBeans и отдать snapshot, а не построить приложение с инфраструктурными зависимостями.

### Как будет устроен Java-код

```text
ExporterMain
  ├─ ConfigLoader -> ExporterConfig
  ├─ JmxConnectionManager
  ├─ Cassandra41Reader -> RawJmxSnapshot
  ├─ MembershipMapper -> MembershipSnapshot
  ├─ SnapshotPoller -> AtomicReference<CollectionState>
  └─ MembershipCollector -> официальный Prometheus HTTPServer
```

- `ConfigLoader`: разбирает YAML, отклоняет неизвестные ключи и некорректные interval/timeout, не печатает секреты.
- `JmxConnectionManager`: создаёт стандартный `JMXConnector`, управляет закрытием соединения и reconnect.
- `Cassandra41Reader`: знает точные ObjectNames и имена атрибутов версии 4.1; возвращает данные и результаты чтения каждой группы.
- `MembershipMapper`: чистая Java-функция без сети, переводит исходные списки/карты в две оси состояния и нормализует endpoints.
- `SnapshotPoller`: последовательно запускает чтение через `scheduleWithFixedDelay`, публикует готовый immutable результат и health текущего чтения.
- `MembershipCollector`: реализует custom collector API client_java; на HTTP scrape только преобразует уже опубликованный snapshot в samples.

Для динамических endpoint-серий предпочитаем custom collector, а не постоянно растущую коллекцию вызовов `Gauge.labelValues(...)`. Тогда удалённый endpoint автоматически исчезает из следующей exposition, а переход статуса не оставляет забытые labelsets. Обычные Counter/Histogram используются для ошибок и длительности самого сбора.

Один poller и ограниченный HTTP thread pool — без Reactor, reactive streams, virtual threads и отдельного scheduler framework. Exporter запускается тем же установленным Java runtime, что и Cassandra, но отдельным процессом; вторую Java на целевую машину не устанавливаем.

### Правила максимальной совместимости

- Production-код и базовые классы зависимостей — Java 8 bytecode (class major version 52). Не использовать records, `List.of`, `java.net.http.HttpClient`, virtual threads и API Java 9+.
- Одного `--release 8` для нашего кода недостаточно: проверять dependency tree, bytecode зависимостей и фактический запуск итогового shaded JAR. Если есть multi-release JAR, различать базовые классы и версионные записи `META-INF/versions/`; последние оценивать на соответствующей JVM.
- Проверить один и тот же JAR на Java 8, 11, 17 и 21: старт, `/metrics`, JMX/RMI, reconnect и timeout. На нашем Cassandra 4.1.12 обязательная комбинация — Cassandra и exporter на Java 11.
- Поддержка runtime не равна поддержке всех Cassandra. На первом этапе контракт MBeans остаётся 4.1.12; для Cassandra 3.x/5.x нужны отдельные adapters и проверки.
- Конфиг читать безопасным конструктором SnakeYAML в простые map/list/scalar и явно валидировать; не разрешать создание произвольных Java-классов из YAML.
- JUnit 5 и AssertJ закрепить в версиях, позволяющих запускать runtime-тесты Java 8. Версия JDK сборочной машины не должна становиться требованием к целевой машине.
- Не зависеть от внутреннего `com.sun.jmx.mbeanserver`, JNI, native-библиотек или дополнительных javaagent. Встроенный HTTP-server должен присутствовать в используемом Java runtime; урезанные custom runtime проверяются отдельно.
- Точные vendor/patch-версии JVM указывать в результатах тестирования. До этих тестов Java 8/11/17/21 — целевая матрица совместимости, а не уже доказанная поддержка.

### Минимальный конфиг exporter

```yaml
cluster: cassandra-lab
observer: cassandra1
jmx:
  service_url: service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi
poll:
  interval_seconds: 5
  timeout_seconds: 3
http:
  listen_address: 127.0.0.1
  port: 9505
collectors:
  membership: true
  local_state: true
  phi: false
  downtime: false
  generation: false
```

Порт 9505 выбран для лаборатории, чтобы не конфликтовать с уже установленными exporter. Параметр timeout — требование к реализации, которое нужно подтвердить тестом RMI; одно поле YAML само по себе не гарантирует прерывание зависшего сетевого вызова.

### Сборка и запуск

```bash
./mvnw clean verify
java -jar target/cassandra-membership-exporter.jar --config exporter.yaml
```

Если Java Cassandra отсутствует в PATH, запускать тем же абсолютным путём к `java`, который используется для Cassandra. Например: `/opt/cassandra-java/bin/java -jar cassandra-membership-exporter.jar --config exporter.yaml`. Путь примерный; отдельный runtime вместе с exporter не поставляется.

Сборка выполняется в нашей среде разработки, например JDK 17 с --release 8. На целевой машине ни JDK для сборки, ни Maven, ни Docker не нужны: копируется готовый JAR и запускается существующей Java Cassandra. Python нужен только нашим лабораторным сценариям.

### Поток данных

Первый вариант — небольшой самостоятельный Java-процесс, по одному экземпляру рядом с каждой Cassandra.

```text
Cassandra A --JMX/RMI--> exporter A --HTTP /metrics--> Prometheus
Cassandra B --JMX/RMI--> exporter B --HTTP /metrics--> Prometheus
Cassandra C --JMX/RMI--> exporter C --HTTP /metrics--> Prometheus

MCAC на каждом узле продолжает собирать существующие метрики.
CQL-probe отдельно проверяет клиентский путь.
```

Стрелка JMX обозначает чтение exporter из Cassandra: exporter не принимает push от Cassandra.

Выбор реализации:

- Java 8 как минимальный bytecode/API baseline. На нашем Cassandra 4.1.12 запускаем exporter той же Java 11. Совместимость с другими runtime подтверждаем тестами.
- Стандартные `JMXConnectorFactory` и `MBeanServerConnection`; без зависимости на внутренние Java-классы Cassandra.
- `getMBeanInfo`, `getAttribute`/`getAttributes` для атрибутов, строго ограниченный `invoke` для безопасных getter-операций с аргументом endpoint.
- Официальная Java-библиотека Prometheus client_java для exposition, конфигурация YAML с валидацией.
- На целевой машине — обычный JAR, запускаемый Java Cassandra. Docker не требуется; в нашей лаборатории контейнеры используются только как тестовая среда.

Это не javaagent: его установка не должна требовать изменения JVM_OPTS Cassandra или её перезапуска при уже доступном JMX. Также отсутствует риск конфликтов порядка javaagent, который мы воспроизвели в сравнительном тесте.

Один exporter читает только один observer. Централизованный процесс с десятками JMX-соединений в первый выпуск не включать.

## 3. Точный JMX-контракт

Сначала проверить таблицу через `getMBeanInfo` на живой Cassandra 4.1.12 и сохранить результат как fixture. Это защищает от ошибок в именах и от различий между атрибутом и операцией.

### StorageService

ObjectName: `org.apache.cassandra.db:type=StorageService`.

| JMX-атрибут | Тип | Применение |
|---|---|---|
| `LiveNodesWithPort` | `List<String>` | Множество доступных endpoints глазами observer |
| `UnreachableNodesWithPort` | `List<String>` | Множество недоступных endpoints |
| `JoiningNodesWithPort` | `List<String>` | Состояние Joining |
| `LeavingNodesWithPort` | `List<String>` | Состояние Leaving |
| `MovingNodesWithPort` | `List<String>` | Состояние Moving |
| `TokenToEndpointWithPortMap` | `Map<String,String>` | Endpoints, известные token metadata, в том числе bootstrap |
| `EndpointWithPortToHostId` | `Map<String,String>` | Связь endpoint и host ID |
| `LocalHostId` | `String` | Идентификация самого observer |
| `GossipRunning` | `boolean` | Работает ли локальный gossip |
| `NativeTransportRunning` | `boolean` | Включён ли локальный CQL transport |
| `Initialized` | `boolean` | Завершена ли соответствующая инициализация Cassandra |
| `OperationMode` | `String` | Локальный режим процесса; не заменяет ring state соседей |

В первой версии использовать варианты `WithPort`: IP без storage port не всегда однозначно идентифицирует Cassandra endpoint.

### FailureDetector

ObjectName: `org.apache.cassandra.net:type=FailureDetector`.

| JMX-атрибут | Применение |
|---|---|
| `PhiValuesWithPort` | Таблица `TabularData`, извлечь Endpoint и PHI через метаданные `CompositeData` |
| `SimpleStatesWithPort` | Дополнительная проверка представления FailureDetector, а не замена источника статуса nodetool |
| `PhiConvictThreshold` | Порог observer для контекста диагностики |

Не подменять решение Cassandra собственным сравнением `phi > threshold`: порог и phi полезны как диагностические сигналы, основной U/D брать из того же источника, что использует `nodetool status`.

### Gossiper — второй этап

ObjectName: `org.apache.cassandra.net:type=Gossiper`.

- Операция `getEndpointDowntime(String address)` — длительность текущего DOWN, возвращается в миллисекундах.
- Операция `getCurrentGenerationNumber(String address)` — generation endpoint.

Это операции JMX, потому что принимают аргумент, хотя имена выглядят как getters. Разрешить только эти конкретные сигнатуры; не создавать универсальный HTTP-прокси для произвольного JMX invoke.

Сначала не разбирать строковый вывод `getAllEndpointStatesWithPort()` или `getEndpointState()`: формат текста нестабилен и для U/D + N/J/L/M не нужен.

## 4. Модель данных и совпадение с nodetool

Хранить **две независимые оси**:

1. Availability: `up`, `down`, `unknown`.
2. Ring state: `normal`, `joining`, `leaving`, `moving`, `unknown`.

Пример: endpoint может одновременно иметь availability=`down` и ring state=`joining`. Нельзя превращать это в единственное `DOWN` и терять второй символ.

Нормальный случай воспроизводит `Status.java`:

- Endpoint есть в LiveNodes → U.
- Иначе есть в UnreachableNodes → D.
- Иначе → неизвестная доступность, а не D.
- Состояние: Joining → J, Leaving → L, Moving → M, иначе N **только при успешном полном чтении необходимых источников состояния**.

Списки изменяются во время опроса и не читаются атомарно. Если endpoint одновременно встретился в Live и Unreachable или в нескольких переходных состояниях:

- Повторить конфликтующий небольшой набор чтений один раз.
- При сохраняющемся конфликте выдать `unknown` для соответствующей оси и отдельный счётчик конфликтов.
- Не скрывать противоречие произвольным выбором «первого» значения. Уточнить в документации, что это осознанное отличие от приоритета списков в nodetool во время гонки.

Множество endpoints строить как объединение token map, host ID map и пяти списков состояния. Это позволяет видеть и bootstrap, и записи, оставшиеся в gossip. Отдельно отмечать присутствие в token metadata: известный gossip endpoint не обязательно действующий владелец токенов.

Таблица `nodetool status` и более широкий список известных endpoints могут различаться. Для побайтового сравнения логики строк проверять только endpoints, выбранные token metadata, а дополнительные endpoints показывать отдельно.

### Адреса и идентичность

- Внутренний ключ endpoint: нормализованный IP + storage port; IPv6 представлять как `[address]:port`.
- Учитывать форматы `hostname/address:port` и `/address:port`, встречающиеся в FailureDetector.
- Не применять `split(":")` или безусловный `split("/")[1]`; отдельно тестировать IPv6 и отсутствие hostname.
- Не делать reverse DNS на каждом scrape.
- `observer` задаётся стабильным идентификатором в конфиге; `LocalHostId` публикуется как metadata. Перезапуск не должен менять observer label.
- Endpoint, host ID и имя контейнера не считать взаимозаменяемыми: после replacement адрес может остаться, а host ID измениться.

## 5. Контракт Prometheus-метрик

Предлагаемый собственный prefix: `cassandra_membership_`, чтобы не конфликтовать с MCAC и Instaclustr.

Основные labels: `cluster`, `observer`, `endpoint`. `instance`/`job` оставляет Prometheus. DC, rack и host ID вынести в отдельные info-метрики, чтобы не умножать metadata во всех рядах.

| Метрика | Тип | Смысл |
|---|---|---|
| `cassandra_membership_endpoint_status{status="up|down|unknown"}` | gauge | One-hot доступность, ровно одно значение 1 на endpoint |
| `cassandra_membership_endpoint_ring_state{state="normal|joining|leaving|moving|unknown"}` | gauge | Независимое one-hot состояние кольца |
| `cassandra_membership_endpoint_in_token_metadata` | gauge | Принадлежность endpoint текущему token metadata observer |
| `cassandra_membership_endpoint_info{host_id,dc,rack}` | gauge | Metadata со значением 1, если прочитана |
| `cassandra_membership_observer_info{host_id}` | gauge | Metadata observer со значением 1 |
| `cassandra_membership_gossip_running` | gauge | Локальный gossip включён |
| `cassandra_membership_native_transport_running` | gauge | Локальный native transport включён |
| `cassandra_membership_initialized` | gauge | Локальная инициализация |
| `cassandra_membership_operation_mode{mode}` | gauge | Локальный режим, например NORMAL |
| `cassandra_membership_endpoint_phi` | gauge | Phi удалённого endpoint, если источник вернул значение |
| `cassandra_membership_phi_convict_threshold` | gauge | Порог текущего observer |
| `cassandra_membership_endpoint_downtime_seconds` | gauge | Текущий непрерывный период недоступности |
| `cassandra_membership_endpoint_generation` | gauge | Generation endpoint |

Для фиксированного endpoint выдавать все значения конечного набора status/state как 0/1. После перехода U→D прежний status=up становится 0 в следующем snapshot, а не продолжает жить из-за забытой серии.

**Downtime — gauge.** В Cassandra 4.1.12 `getEndpointDowntime()` возвращает время с момента помещения в `unreachableEndpoints`, иначе 0. Это не накопленный downtime за жизнь процесса; имя `_total` и тип counter здесь неверны.

Не придумывать phi для самого observer и не заполнять отсутствующий phi нулём. Не интерпретировать generation как количество рестартов без отдельного алгоритма и его проверки.

### Здоровье самого сбора

Нужны отдельные метрики, не смешанные с состоянием Cassandra:

- `cassandra_membership_collection_success{collector}` — успех последнего опроса компонента, 0/1.
- `cassandra_membership_snapshot_timestamp_seconds{collector}` — время последнего успешного snapshot.
- `cassandra_membership_collection_duration_seconds{collector}` — histogram длительности.
- `cassandra_membership_collection_errors_total{collector,reason}` — ошибки с ограниченным набором reason.
- `cassandra_membership_capability{capability}` — поддержка optional возможностей.
- `cassandra_membership_state_conflicts_total{axis}` — устойчивые конфликты после повторного чтения.
- `cassandra_membership_expected_observer_info{observer}` — такую ожидаемую конфигурацию лучше формировать из inventory/service discovery снаружи, а не считать, что исчезнувший exporter сам сообщит о себе.

Не ставить текст exception, URL с credentials или произвольные сообщения в labels. HTTP `up` Prometheus означает доступность exporter; успешность JMX надо проверять отдельно.

## 6. Опрос, кэш и отказы

Начальные настройки для стенда: опрос каждые **5 секунд**, Prometheus scrape каждые **10 секунд**, бюджет одного опроса около **3 секунд**, допустимая свежесть около **15 секунд**. Это исходные параметры для измерения, не обещание SLA.

1. Фоновый poller читает данные JMX независимо от HTTP-запросов Prometheus.
2. По возможности использовать `getAttributes`, но проверять наличие каждого атрибута в ответе: отсутствие не равно пустому списку.
3. Формировать immutable snapshot и публиковать его атомарно.
4. Разделить обязательные источники availability/ring state и optional phi/metadata/downtime.
5. Ошибка optional phi не должна уничтожать уже прочитанный U/D snapshot.
6. Ошибка любого из пяти обязательных списков не должна превращаться в Normal/UP по умолчанию.
7. При ошибке группы немедленно публиковать collection_success=0 и не выдавать старые значения этой группы как свежие. Допустимо хранить прошлый snapshot для диагностики, но health-метрики и timestamps должны явно показывать его возраст.
8. В HTTP exposition старые state-серии не обслуживать после ошибки соответствующей группы; последний достоверный результат остаётся в истории Prometheus. Не создавать фиктивные массовые DOWN при обрыве JMX.
9. При успешном чтении отсутствие endpoint из всех актуальных источников удаляет его серии из нового snapshot. Исторические серии остаются в Prometheus; отдельное ожидаемое inventory ловит непреднамеренное исчезновение.
10. Ввести reconnect с backoff и jitter; хранить одно соединение, а не создавать его на каждый HTTP scrape.

При изменении состояния во время poll нет общей транзакции JMX. Гарантировать честный возраст данных и обработку конфликтов, а не «абсолютно атомарную картину кластера».

### Таймауты

У `Future.cancel()` нет гарантии остановки зависшего RMI-чтения. Поэтому отдельно проверить connect/read timeout на каждой заявленной версии Java и соответствующем RMI provider; не считать любой environment key универсально работающим.

Ограничить число worker threads, не запускать новый зависающий poll на каждом тике. При зависании чтения HTTP должен продолжать отдавать health-метрики. В крайнем случае watchdog завершает только exporter, а supervisor его перезапускает. Это проверяется fault test зависшего JMX, а не только отказом TCP-соединения.

## 7. Что сделать в первом MVP

Первый полезный результат должен работать без расширенного разбора всего gossip state:

1. Чтение `StorageService` и обнаружение capabilities.
2. U/D/unknown и N/J/L/M/unknown по endpoint.
3. Локальные GossipRunning и NativeTransportRunning.
4. Метрики здоровья опроса и свежести.
5. Снимок состояния для диагностики, например локальный `/debug/snapshot`, выключенный по умолчанию и без credentials.
6. Конфиг одного observer, HTTP endpoint, контейнер и Compose overlay для нашего стенда.

Phi, generation, downtime и DC/rack добавить вторым небольшим этапом. Effective ownership и load в MVP не нужны для обнаружения состояния узлов.

Если позднее понадобится весь вывод `nodetool status`, добавить:

- load с точной проверкой единиц и формата `LoadMapWithPort`;
- количество токенов как число отображений на endpoint;
- Host ID, DC/rack через соответствующий JMX-контракт;
- ownership отдельно по явно заданным keyspaces, с пониманием replication strategy.

Не экспортировать каждый token отдельным label и не опрашивать effectiveOwnership для всех keyspaces на каждом тике.

## 8. План реализации по этапам

| Этап | Работа | Результат и условие завершения |
|---|---|---|
| 0. Контракт | Снять MBeanInfo и реальные ответы нашего 4.1.12 | Fixtures типов/имён; таблица attributes/operations подтверждена живым JMX |
| 1. Ядро модели | Нормализация адресов, объединение endpoints, независимые оси состояния | Unit tests на U/D/unknown, N/J/L/M, конфликты, IPv6 |
| 2. JMX adapter | Соединение, чтение allowlist, reconnect, bounded timeout | Успешный poll без внутренних Cassandra JAR; ошибка опроса видна отдельно |
| 3. Exposition | Snapshot, метрики, удаление старых labelsets, health | `promtool check metrics`; повторные HTTP scrapes не создают дополнительные JMX poll |
| 4. MVP на стенде | Один exporter на observer, затем три | Снимки совпадают с nodetool в устойчивом состоянии; ни один существующий exporter не заменён |
| 5. Fault tests | Pause, CQL off, JMX failure, network asymmetry, recovery | Автоматические проверки и доказательства всех критериев ниже |
| 6. Диагностика | Phi, generation, downtime, metadata | Optional failures не ломают основной snapshot; единицы и gauge/counter проверены |
| 7. Проверка реализации | Лабораторный прогон, ресурсы и reconnect | Измеренный overhead и отсутствие роста зависших потоков/соединений |

После этапа 0 оценить объём работ точнее. Текущий результат — рабочий exporter с проверенной семантикой метрик и воспроизводимыми лабораторными тестами.

## 9. Испытания с конкретными ожидаемыми результатами

### Unit tests

- Три healthy endpoint → U/N; отсутствие в live/unreachable → unknown, не DOWN.
- DOWN + Joining остаются двумя разными значениями; не теряется DJ.
- Успешно пустые transition lists → Normal для известных endpoints.
- Ошибка чтения transition list → неизвестная соответствующая ось, а не Normal.
- Конфликт U/D или J/L/M → ограниченный retry, затем unknown и conflict counter.
- IPv4, IPv6, hostname/IP, port, replacement с новым host ID.
- Удаление endpoint и labels из следующего snapshot.
- Нет phi → нет выдуманного phi=0.
- Downtime 0 → рост → 0 после recovery; generation может измениться после рестарта.

### Integration / fault tests на трёх узлах

| Сценарий | Ожидаемый результат |
|---|---|
| Все узлы здоровы | Все observer сообщают U/N для действующих ring endpoints; collection_success=1 |
| Пауза node3 | Node1 и node2 после сходимости сообщают D/N; exporter node3 может оставаться доступным, но его JMX poll перестаёт быть успешным |
| Возобновление node3 | D→U, downtime сбрасывается, collection health восстанавливается |
| Только `disablebinary` node3 | Gossip остаётся U/N; локальная NativeTransportRunning node3=0; CQL-probe показывает отказ |
| Отказ JMX при работающем exporter | Prometheus up=1, collection_success=0, данные не выдаются за свежие; массового DOWN нет |
| Неверные JMX credentials | Auth error в ограниченном reason, credentials не попадают в лог/метрики |
| Зависание JMX | HTTP не висит вместе с poll; нет неограниченного создания worker threads |
| Ошибка optional PhiValues | Availability/ring state остаются доступны, optional collector показывает ошибку |
| Ассиметричная сеть A↔B | Фиксируются разные мнения observer об endpoint, без раннего агрегирования |
| Join / leave / move | J/L/M видны, а затем переходят в N либо endpoint исчезает из ring inventory |

Сетевые фильтры применять только к лабораторным контейнерам и адресам. Обычную pause-проверку не выдавать за проверку network partition. В трёхузловом кластере gossip может донести сведения обходным путём: для асимметрии спроектировать точную схему фильтрации и подтвердить её пакетами/соединениями.

Joining/leaving/moving часто кратковременны. Для детерминированного интеграционного теста использовать отдельные временные узлы/volumes или отдельный кластер; не decommission существующие узлы пользователя. Moving сначала проверить на поддерживаемой для операции конфигурации токенов — не предполагать, что move применим к нашему vnode-кластеру с 16 tokens.

Во всех fault tests — `finally` для восстановления. После них: три UN, работающий native transport, CQL read/write с нужной consistency и проверка отсутствия забытых network filters.

## 10. Приёмка и ограничения первого релиза

Функционал считается готовым для лабораторного использования, когда:

- На 4.1.12 состояния устойчивого кластера совпадают с логикой nodetool для ring endpoints.
- Разные observer сохраняются отдельными рядами.
- DOWN, unknown и ошибка чтения JMX не смешиваются.
- Ring state остаётся независимым от availability.
- Нет устаревших positive-state рядов после смены состояния/удаления endpoint.
- IPv6 и storage port покрыты тестами.
- Failures и восстановление автоматизированы и воспроизводимы.
- HTTP отвечает при проблемах JMX; число потоков/соединений ограничено.
- Нагрузка измерена, а не оценена только временем отдачи кэша `/metrics`.
- MCAC и текущие dashboards продолжают работать.

Длительный лабораторный прогон проверяет корректность реализации; подготовка к production в текущую работу не входит.

Объём рядов по pair-метрикам растёт примерно как O(N²), потому что каждый observer описывает соседей. Первый масштабный тест провести, например, на синтетических fixtures с 10/100/500 endpoints и реальном стенде доступного размера; не добавлять токены, сообщения ошибок или полные gossip payloads в labels.

## 11. Предлагаемая структура изменений

```text
membership-exporter/
  pom.xml
  src/main/java/.../jmx/          # adapter, discovery, read allowlist
  src/main/java/.../model/        # endpoint, две оси состояния, snapshot
  src/main/java/.../polling/      # schedule, reconnect, bounded failures
  src/main/java/.../prometheus/   # exposition из snapshot
  src/test/                      # unit tests и JMX fixtures 4.1.12
  Dockerfile
monitoring/membership/            # sample config и лабораторные сценарии
compose.membership.yaml
research/membership-validation/   # результаты интеграционных испытаний
```

Порядок работы: сначала контракт и MVP с U/D + N/J/L/M и честными ошибками, затем реальные fault tests, затем дополнительные метрики и проверка реализации.

## Источники, сверенные при подготовке

- [Логика nodetool status в Cassandra 4.1.12](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/tools/nodetool/Status.java).
- [StorageServiceMBean 4.1.12](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/service/StorageServiceMBean.java).
- [StorageService: происхождение LiveNodes/UnreachableNodes](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/service/StorageService.java).
- [FailureDetectorMBean 4.1.12](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/gms/FailureDetectorMBean.java).
- [GossiperMBean 4.1.12](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/gms/GossiperMBean.java).
- [Gossiper: реализация downtime и generation](https://github.com/apache/cassandra/blob/cassandra-4.1.12/src/java/org/apache/cassandra/gms/Gossiper.java).
- Локальные доказательства предыдущего сравнения: `research/comparison-results.json`, `research/comparison-evidence.txt`, `research/exporter-comparison.txt`.
