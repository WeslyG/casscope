# Проверка первого MVP — 5 ноября 2026

## Артефакт

- Исходники: `membership-exporter/`.
- JAR: `membership-exporter/target/cassandra-membership-exporter.jar` (не включён в Git).
- SHA256 окончательного JAR: `64d0356c83b1e95e9421d5760b4f757885be7bffd5b02aa286dae2e2751cbda3`.
- 1264 базовых class-файла: максимальная class version 52 (Java 8). Версионные записи multi-release JAR и module-info не считаются базовыми Java 8 классами.
- Maven Wrapper 3.9.11; чистая сборка `./mvnw -B -ntp clean verify` на JDK 17 — успешна.

## Автоматические тесты

10 tests, 0 failures, 0 errors, 0 skipped. Итоги: `unit-results.json`.

- Один реальный JMX/RMI batch-read при 200 HTTP scrape; нет per-attribute getter/invoke.
- 200 конкурирующих вызовов при зависшем Reader не запускают второй Reader.
- Реальный RMI timeout в отдельной JVM открывает circuit. HTTP остаётся доступным, число batch-операций на тестовом сервере после открытия circuit не растёт. Учитывается возможный внутренний retry JDK: максимум два серверных batch-вызова в тесте, а не бесконечные попытки exporter.
- Медленное успешное чтение увеличивает паузу; ошибка не выдаёт старые endpoint-state ряды как свежие.
- Проверены независимость U/D и N/J/L/M, конфликты, отсутствующие атрибуты, cap, IPv4/IPv6/mapped IPv4, безопасный YAML и backoff.

## Живая Cassandra 4.1.12

Окончательный JAR работает на Java 11 официального Cassandra image. Один observer — node1. Cassandra не пересобиралась и не перезапускалась при установке exporter.

`lab-results.json`:

- 400 HTTP scrape за 3,28 секунды → **0 дополнительных application JMX reads**.
- Пауза node3 → endpoint node3 получил status=down, ring state остался независимым.
- Unpause → status=up; восстановление подтверждено.
- После теста три узла UN/healthy; отдельное чтение `lab_smoke.probe` с CONSISTENCY ALL успешно.

## Runtime-матрица

Один и тот же окончательный JAR: JMX/RMI к Cassandra 4.1.12 и HTTP `/metrics` успешно проверены на:

- Java 8: Temurin 1.8.0_504.
- Java 11: Java официального Cassandra image, основной лабораторный экземпляр.
- Java 17: Temurin 17.0.21.
- Java 21: Temurin 21.0.13.

Подробнее о дополнительных JVM: `runtime-matrix.json`. Это Linux runtime checks, не сертификация всех JVM vendor/OS и не подтверждение Cassandra 3.x/5.x.

## Ограниченная CQL-нагрузка

Отдельный read-only прогон: 900 SELECT за 44,95 секунды, лимит 20 запросов/с, одна существующая строка `lab_smoke.probe`. Успешные ответы проверялись. P95 ~1,94 мс, максимум ~13,86 мс.

Во время этого прогона application JMX reads выросли с 11 до 12; collection_success оставался 1. Последняя длительность JMX poll ~2,23 мс. Сырые данные — `cql-load.json`, `before-cql-load.prom`, `after-cql-load.prom`. Нагрузочный прогон предшествовал финальной правке нормализации IPv4-mapped IPv6; код опроса/планирования и лимиты не менялись.

Это маленький ненасыщенный кластер и умеренная нагрузка. Нет основания заявлять нулевое влияние в production, гарантировать P99 или переносить эти latency на сотни узлов. Ключевое доказательство защиты — ограниченная частота чтений, отсутствие связи HTTP→JMX и остановка повторов после timeout.

## Состояние стенда

`cassandra-lab-membership` оставлен работающим возле node1, `/metrics` на `127.0.0.1:9505` в сетевом пространстве node1. На хост этот порт не публиковался. Лабораторный контейнер имеет лимиты 128 MiB и 0,25 CPU; один наблюдавшийся idle snapshot — около 43 MiB RAM, это не максимум под нагрузкой.

Все временные runtime-matrix exporter удалены. Узлы Cassandra восстановлены. Контейнеры — только способ испытания: доставляемый продукт обычный standalone JAR для существующей Java Cassandra.
