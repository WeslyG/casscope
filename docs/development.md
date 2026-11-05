# Разработка и выпуск

## Структура

```text
membership-exporter/
  pom.xml, mvnw, mvnw.cmd
  src/main/java/local/cassandra/membership/
    Config.java                 # YAML и проверка ограничений
    JmxReader.java              # allowlist StorageService, соединение
    Snapshot.java, Endpoint.java # immutable модель и нормализация
    Poller.java, PollPolicy.java # единственный poller, backoff/circuit
    MembershipCollector.java    # exposition из snapshot
    MetricsServer.java          # HTTP exposition и readiness
    Main.java                   # CLI и запуск
  src/test/                     # unit и локальные JMX/RMI/HTTP tests
examples/                       # YAML, Prometheus alerts и systemd
molecule/recovery/              # отдельные реальные Cassandra e2e
.github/workflows/release.yml
```

Production-код и все базовые классы JAR должны поддерживать Java 8. Не использовать records, Java 9+ collections factories, внутренние Cassandra JAR или javaagent. Multi-release entries проверяются отдельно от базовых классов. `sun.rmi` properties и глобальная socket factory настраиваются только в standalone JVM Casscope, не в Cassandra.

## Проверки

```sh
cd membership-exporter
./mvnw -B -ntp clean verify
cd ..
python3 scripts/check_release.py --jar membership-exporter/target/casscope-1.1.0.jar
java -jar membership-exporter/target/casscope-1.1.0.jar --version
```

JDK нужен для сборки; на целевой машине достаточно Java runtime. Maven Wrapper скачивает закреплённый Maven, зависимости требуют доступа к Maven Central при первом запуске. Python нужен только вспомогательным developer scripts и не нужен для работы JAR.

`verify` запускает тесты модели/config, реального локального JMX/RMI, HTTP scraping, отсутствия параллельных reads, timeout/circuit и backoff. Внешняя Cassandra и Docker не нужны. Отсутствие тестов считается ошибкой сборки. Полные Cassandra e2e/Molecule выполняются отдельно и не входят в release gate.

При изменении collectors:

- Не добавлять JMX в HTTP path или обход всех MBeans как fallback.
- Не создавать per-endpoint RPC без отдельного бюджета и измерений.
- Не добавлять немедленные retries после timeout; клиент не отменяет серверную работу.
- Различать пустой ответ, отсутствующий атрибут, конфликт и ошибку связи.
- Проверять удаление старых labelsets и ограничения числа endpoints.
- Не увеличивать частоту опроса ради теста и не обходить safety limits.
- Не выдавать synthetic/fake fixture за доказательство поведения Cassandra.

Измеренные результаты, логи, бинарники и исходные условия экспериментов хранить в уникальных run directories под `.artifacts/casscope/`. Они исключены из Git. Перед commit проверять staged files; результаты не коммитить. Не перезаписывать failed run успешным повтором.

## Версия и release workflow

Единственный источник версии — `membership-exporter/pom.xml`. Текущая версия **1.1.0**, имя выходного файла `casscope-1.1.0.jar`; версия также записана в manifest и доступна через `--version`.

Workflow запускается на PR, push main и тег `v*`:

1. Проверяет стабильную версию x.y.z и совпадение тега с `v<version>`.
2. Выполняет `clean verify` на Java 8, 11, 17 и 21.
3. Проверяет manifest, базовый bytecode Java 8 и запуск `--version` каждого JAR.
4. Сохраняет тестовые отчёты как Actions artifacts. Для релиза выбирает уже проверенный JAR из Java 17 job.
5. Только после успеха всей матрицы создаёт либо возобновляет draft release, загружает один JAR, проверяет список assets и SHA256 обратным скачиванием, затем публикует draft по release ID.

Публикация идёт встроенным GitHub token с `contents: write` только в release job. В test job права read-only. Actions закреплены commit SHA. Исходный код не публикует результаты локальных экспериментов.

Для выпуска версии сначала закоммитьте проверенные изменения версии и убедитесь, что тег указывает именно на этот commit:

```sh
# Сначала обновить POM, примеры и проверить изменения.
git tag v1.1.0
git push origin main
git push origin v1.1.0
```

Эти команды относятся к осознанной публикации: push тега запускает release workflow. Само добавление remote или локального тега релиз не публикует. Не передвигайте выпущенный тег и не заменяйте опубликованный asset; для исправления используйте новую версию. Ошибка загрузки оставляет draft для разбора. Повторный запуск может заменить asset только в незавершённом draft после повторного прохождения тестов; опубликованный релиз workflow отвергает.

В release добавляется только JAR. Автоматические `Source code` архивы интерфейса GitHub могут отображаться отдельно; workflow не загружает дополнительные ZIP/TAR, конфиги или test reports как release assets.

Проверки документации и Prometheus выполняются отдельным быстрым CI job: `python3 scripts/check_docs.py`, `promtool check config examples/prometheus/prometheus.yml`, `promtool test rules tests/prometheus/casscope-alerts.test.yml`. Отдельные реальные испытания описаны в [testing.md](testing.md).
