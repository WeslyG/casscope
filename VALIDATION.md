# Проверка запуска 2026-11-05

- Контроллер: `cassandra-lab-devcontainer`, Ansible 14, core 2.21.6, Python 3.14.10.
- Запуск `docker compose up` выполнен от vscode внутри контроллера через Docker socket.
- Cassandra 4.1.12, три healthy-контейнера, все три узла UN, dc1, rack1/rack2/rack3.
- Все три узла подтвердили одинаковую schema_version: `07a33fe3-ebe7-3c34-b049-29c8f3afd546`.
- RF=3, CONSISTENCY ALL: INSERT и SELECT успешно вернули `1 | ready`.
- Первоначальные попытки DDL/CQL дали таймауты и INCOMPATIBLE_SCHEMA во время
  распространения схемы. После согласования схемы проверка ALL прошла за секунду.
- Данные и тестовый keyspace сохранены; кластер оставлен работающим.
