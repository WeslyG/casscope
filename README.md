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
