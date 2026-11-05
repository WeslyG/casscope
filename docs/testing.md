# Проверки Casscope

## Проверки релиза без внешнего кластера

```sh
cd membership-exporter
./mvnw -B -ntp clean verify
cd ..
python3 scripts/check_release.py --jar membership-exporter/target/casscope-1.1.0.jar
python3 scripts/check_docs.py
promtool check config examples/prometheus/prometheus.yml
promtool test rules tests/prometheus/casscope-alerts.test.yml
```

Java-тесты проверяют модель, конфигурацию, HTTP, локальный JMX/RMI, свежесть, timeout, backoff и отсутствие параллельных опросов. Внешняя Cassandra и Docker не нужны. Тест восстановления выдерживает настоящую минутную паузу. CI выполняет эти тесты на Java 8/11/17/21, проверяет manifest, базовый bytecode Java 8 и запуск JAR. Promtool проверяет обе ветви алерта отказа сбора, подавление недостоверных состояний и восстановление.

## Отдельный Molecule-сценарий

E2e не входит в release workflow. Его запускают на выделенном Linux Docker controller с доступом к socket:

```sh
python3 scripts/run_recovery_lab.py --jar membership-exporter/target/casscope-1.1.0.jar
```

Нужны Python controller, Molecule 26.10.0, ansible-core 2.21, community.docker 5.4.0, Docker SDK 7.2 и доступ к Docker engine. Практически проверен Python 3.14. Минимум 3500 MiB свободной памяти требуется после остановки известного локального стенда. Образы должны быть заранее доступны controller: Cassandra 4.1.12 и Prometheus 3.6.0 с digest из scripts/recovery_lab.py, maven:3.9.11-eclipse-temurin-11 и eclipse-temurin:8-jre, 17-jre, 21-jre. Docker нужен только для испытаний.

Сценарий использует одну настоящую Cassandra 4.1.12/Java 11 (2 CPU, 2 GiB RAM, heap 1 GiB), один exporter (0.5 CPU, 128 MiB, heap 64 MiB) и Prometheus (0.5 CPU, 256 MiB). Сеть изолирована, host ports не публикуются. JMX без authentication разрешён только в тестовой сети. Независимый Reference компилируется из исходников и читает настоящие MBeans.

Molecule выполняет syntax, create, prepare, converge, idempotence, side_effect, verify, cleanup и destroy. Проверяются:

- Один scrape job /metrics; HTTP 200 с collection_success=0 и диагностикой при отказе JMX.
- Отсутствие старых состояний Cassandra в HTTP и текущем PromQL, поступление диагностических counters.
- Readiness 503, реальный алерт при up=1 и collection_success=0.
- Минимальные retry-паузы 60/120 секунд, server-side getAttributes logging и отсутствие scrape-triggered JMX.
- Потеря ответов после подтверждённого dispatch; восстановление без смены container ID и StartedAt.
- Отключение CQL при доступном JMX: collection_success=1, native_transport_running=0 и service alert.
- Отдельная остановка экспортера: up=0, collection alert и восстановление после явного запуска. Она выполняется после проверки JMX recovery без restart.
- Тот же JAR SHA256 на Java 8/17/21; основной exporter работает на Java 11.

NET_ADMIN helper меняет только точное firewall-правило в namespace тестового Cassandra-узла и удаляет его в finally. Правила хоста не меняются. Runner временно останавливает только явно перечисленные контейнеры cassandra-lab, сохраняет данные и восстанавливает их после успешного teardown. Посторонние контейнеры не трогает. Failed fixture сохраняется для разбора; прежний стенд в таком случае не запускается автоматически, чтобы не исчерпать память.

## Артефакты

Каждый запуск создаёт уникальный каталог .artifacts/casscope/, исключённый из Git. В нём сохраняются точный JAR и SHA256, замороженная обвязка и конфиги, manifest, HTTP bodies, Prometheus API, assertions, JVM versions, server-side JMX logs, inspect и физические Cassandra logs/GC. Отсутствие реальных GC-файлов считается ошибкой capture. Phases имеют SHA256SUMS; после capture создаётся финальный индекс.

Fixture удаляется только после успешных проверок и сохранения доказательств. result.json и wrapper-result.json описывают результат проверок и завершение lifecycle отдельно. Не перезаписывайте failed run успешным повтором и не коммитьте артефакты. Retained fixture можно продолжить с --resume-fixture <run-directory> только с тем же JAR SHA256; новая попытка сохраняет происхождение.

Этот профиль проверяет функциональность и частоту обращений JMX. Для оценки влияния на CQL нужны отдельные нагрузочные A/B и endurance-прогоны. Authentication/TLS, большая топология и другие версии Cassandra требуют отдельных испытаний.
