# Cassandra lab

## Новый membership exporter

Реализован первый Java-8-совместимый MVP: [исходники и ограничения](membership-exporter/README.md),
[конфиг](membership-exporter/exporter.yaml). Обычный JAR запускается существующей
Java Cassandra, Docker на целевой машине не нужен. Эксплуатационная интеграция не добавлялась.

Защита нагрузки: один batch JMX read за цикл, пауза 30 секунд, backoff при
замедлении/ошибках, отсутствие дополнительных JMX-чтений при HTTP scrape;
при socket timeout — остановка опроса до перезапуска exporter.
Лабораторные доказательства: `research/membership-validation/`.

Локальный кластер Cassandra 4.1.12: три узла, dc1, три rack, постоянные Docker volumes.
Контроллер — готовый devcontainer Ansible 14 (`ansible-infra:ansible14-2026.11.04`).
Docker CLI внутри контроллера использует сокет Docker хоста.

Открытие в VS Code: **Dev Containers: Reopen in Container**.

В терминале devcontainer, каталог `/workspaces/cassandra-lab`:

```bash
docker compose up -d --wait --wait-timeout 900
docker compose ps
docker compose exec cassandra1 nodetool status
docker compose exec cassandra1 cqlsh
docker compose exec -T cassandra1 cqlsh --request-timeout=120 < schema.cql
docker compose exec cassandra1 nodetool describecluster
docker compose exec -T cassandra1 cqlsh --request-timeout=60 < smoke.cql
docker compose logs --tail=100 cassandra1
docker compose stop
docker compose start
```

Запущенный контроллер называется `cassandra-lab-devcontainer`. Вход с хоста:

```bash
docker exec -it -u vscode -w /workspaces/cassandra-lab cassandra-lab-devcontainer bash
```

После `schema.cql` дождаться единой schema version для всех трёх узлов в
`nodetool describecluster`, затем запускать `smoke.cql`. Первичное распространение
схемы на этом Docker-хосте может занимать несколько минут.
`smoke.cql` проверяет запись/чтение с RF=3 и CONSISTENCY ALL. В `lab_smoke.probe`
остаётся одна тестовая строка `1, ready`.

CQL доступен с хоста на `127.0.0.1:9042`; внутри Docker сети — `cassandra1:9042`.
Для клиентского доступа из devcontainer можно пользоваться `docker compose exec cassandra1 cqlsh`.
Это локальное учебное окружение без аутентификации и TLS. На каждый узел установлен
лимит 2 ГБ RAM и heap 1 ГБ. Данные сохраняются при остановке и `docker compose down`.
`docker compose down -v` удаляет данные: запускать только при намеренном сбросе.

Образ Cassandra: https://hub.docker.com/_/cassandra

## Проверка exporter

Установлен Instaclustr cassandra-exporter 0.9.14 в standalone-режиме для cassandra1.
Он работает в сетевом пространстве узла и читает локальный JMX без изменения
настроек JMX Cassandra. Метрики слушают порт 9500 внутри этого пространства;
на хост порт не опубликован. Prometheus не устанавливался.

Воспроизводимый запуск (Python на хосте либо внутри devcontainer):

```bash
python monitoring/download-exporter.py
docker compose -f compose.yaml -f compose.monitoring.yaml up -d --build --no-deps exporter
```

Чтение метрик с хоста:

```bash
docker exec cassandra-lab-cassandra1-1 python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9500/metrics').read().decode())"
```

Проверка паузой cassandra3 подтвердила `endpoint_active: 1 → 0 → 1`, рост phi и
downtime. Полного состояния кольца N/J/L/M exporter не отдаёт. Подробности и
границы проверки: `research/exporter-investigation.txt`; сырые выборки endpoint
метрик и nodetool status сохранены рядом. Для мониторинга глазами каждого узла
нужен отдельный exporter/scrape target на каждом узле; установлен один для проверки.
Также обнаружен дефект разбора scope `IP_7000`: часть Connection-метрик, включая
GossipMessageCompletedTasks, пропускается с предупреждениями в логе exporter.

## Сравнение exporter на Cassandra 4.1.12

Полный обзор: `research/exporter-comparison.txt`. Измерения:
`research/comparison-results.json`; доказательства fault tests:
`research/comparison-evidence.txt`. Полные `.prom` snapshots остаются локально
в `research/` и не включаются в Git. Версии и SHA256 зафиксированы рядом.

В текущем стенде node2 запускает MCAC 0.3.6-4.1-beta1 и Instaclustr agent 0.9.14.
На node1 подключены standalone Instaclustr, JMX Exporter 1.7.0, Criteo 2.3.8
и Casspoke 1.3.6. Два javaagent на node2 используются для сравнения, не как
рекомендация совместного production-развёртывания. Порядок агентов важен.

Воспроизведение с хоста:

```bash
python monitoring/download-comparison.py
docker compose -f compose.yaml -f compose.monitoring.yaml -f compose.comparison.yaml up -d --build --wait --wait-timeout 900
python monitoring/compare.py
python monitoring/summarize.py
```

Отдельный тест Casspoke с ожиданием реакции на отключение CQL:
`pwsh -File monitoring/probe-test.ps1` с хоста.

На Windows использовать `py -3` вместо `python`. Fault tests автоматически
приостанавливают node3 и отключают его CQL, затем восстанавливают в `finally`.
Запускать только на этом учебном стенде. Casspoke выполняет CQL read/write probes.
Базовый `docker compose up` без overrides вернёт node2 к образу без агентов;
для сохранения сравнительного окружения использовать все три Compose-файла.
