# Cassandra lab

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
