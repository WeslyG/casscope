# Casscope

Casscope экспортирует в Prometheus доступность и состояние узлов Cassandra **глазами каждого наблюдателя**. Читает JMX/RMI и работает отдельным Java-процессом рядом с Cassandra.

- Независимые состояния: `up/down/unknown` и `normal/joining/leaving/moving/unknown`.
- Локальные признаки работы gossip, native transport и инициализации.
- Контроль свежести, ошибок и частоты опроса.
- Один исполняемый JAR; Docker и дополнительная Java на целевой машине не требуются.

## Требования

- Существующий Java runtime Cassandra, **Java 8 или новее**, с JMX/RMI и встроенным HTTP server.
- Доступ к JMX наблюдаемой Cassandra, обычно `127.0.0.1:7199`.
- Контракт MBeans практически проверен на **Apache Cassandra 4.1.12**. Поддержка других Cassandra major не заявляется.
- Базовый запуск проверен на Linux с Java 8/11/17/21; это не гарантия всех vendor/OS/TLS-конфигураций.

## Запуск

Скачайте `casscope-1.0.0.jar` из [Releases](https://github.com/WeslyG/casscope/releases) и скопируйте [пример конфигурации](examples/exporter.yaml).

```sh
/path/to/cassandra-java/bin/java -Xms16m -Xmx64m \
  -jar casscope-1.0.0.jar --config exporter.yaml
```

Используйте тот же путь к `java`, который уже используется Cassandra. Это отдельный процесс, а не javaagent; перезапуск Cassandra для установки Casscope при доступном JMX не требуется.

```sh
java -jar casscope-1.0.0.jar --version
curl http://127.0.0.1:9505/metrics
```

HTTP по умолчанию слушает только loopback. Для удалённого Prometheus задайте нужный `listen_address` и ограничьте сетевой доступ. Встроенные authentication/TLS HTTP в Casscope не настроены.

## Принцип работы и нагрузка

Один фоновый poller читает девять атрибутов `StorageService` одним `getAttributes`: пять списков membership и четыре локальных атрибута. Результат публикуется как immutable snapshot. `/metrics` читает только этот snapshot и не обращается к Cassandra.

По умолчанию следующий цикл начинается через **30 секунд после завершения предыдущего**, с положительным jitter до 10%. Интервалы меньше 15 секунд отклоняются. Нет запросов на каждого endpoint, token maps, table metrics, ownership, полного сканирования MBeans и управляющих операций.

При ошибке или медленном успешном чтении задержка увеличивается до 300 секунд. Пропущенные циклы не догоняются; параллельные poll workers не создаются. При socket timeout circuit открывается: **новые опросы прекращаются до перезапуска Casscope**. Таймаут клиента не отменяет уже начатую работу на Cassandra, поэтому бесконечные автоматические повторы здесь запрещены.

Это ограничивает нагрузку, но не делает чтение бесплатным: списки растут с размером кластера. `jmx_reads_total` считает application-вызовы, а не весь RMI protocol/reconnect-трафик.

## Конфигурация и метрики

Все параметры, defaults и ограничения описаны в [configuration.md](docs/configuration.md).

Метрики имеют prefix `cassandra_membership_` и labels `cluster`, `observer`; endpoint-метрики также содержат IP и storage port в `endpoint`.

```text
cassandra_membership_endpoint_status{cluster="production",observer="cassandra-01",endpoint="10.0.0.2:7000",status="down"} 1
cassandra_membership_endpoint_ring_state{cluster="production",observer="cassandra-01",endpoint="10.0.0.2:7000",state="normal"} 1
```

Для status/state публикуется полный набор значений 0/1. Состояние кольца независимо от доступности: например, DOWN не стирает JOINING. Разные observer описывают свои представления; их нельзя заранее сводить к одному глобальному UP/DOWN.

При ошибке опроса или устаревании snapshot endpoint-series не публикуются. Это **не массовый DOWN**. Проверяйте `collection_success`, `snapshot_age_seconds`, `circuit_open` и доступность самого HTTP endpoint.

`/-/healthy` проверяет HTTP-процесс, а не Cassandra. Gossip UP не гарантирует доступность CQL или корректность репликации; клиентская CQL-проверка выполняется отдельно. Не считайте собственное мнение узла о себе независимым подтверждением его доступности.

## systemd

Готовый пример: [casscope.service](examples/systemd/casscope.service). Установка и важные особенности перезапуска: [systemd.md](docs/systemd.md).

## Разработка

Исходники Java и Maven Wrapper находятся в `membership-exporter/`. Для сборки нужен JDK; рекомендуется JDK 17, результат компилируется с `--release 8`.

```sh
cd membership-exporter
./mvnw -B -ntp clean verify
java -jar target/casscope-1.0.0.jar --version
```

На Windows используется `mvnw.cmd`. Тесты включают настоящий локальный JMX/RMI server, HTTP scrape, timeout и проверки ограничения запросов; внешний кластер Cassandra не требуется.

[development.md](docs/development.md) описывает структуру кода, правила изменения collectors, проверки и сборку релиза. Конфиги, изменения рабочего дерева и измеренные результаты лабораторных испытаний сохраняйте отдельно; `.artifacts/` исключён из Git.

## Релизы

GitHub Actions запускает тесты на Java **8/11/17/21** для pull request, push в `main` и тегов `v*`. Публикация выполняется только для тега, совпадающего с версией проекта, например `v1.0.0`.

После успеха всех тестов публикуется **один загружаемый asset: `casscope-1.0.0.jar`**. Архивы, конфиги, test reports и результаты экспериментов в release assets не добавляются. Полные Cassandra e2e в release workflow не запускаются.
