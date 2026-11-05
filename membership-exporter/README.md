# Java-модуль Casscope

Основная документация: [README](../README.md).

- [Конфигурация](../docs/configuration.md)
- [Запуск через systemd](../docs/systemd.md)
- [Разработка и релизы](../docs/development.md)

```sh
./mvnw -B -ntp clean verify
java -jar target/casscope-1.1.0.jar --version
java -Xms16m -Xmx64m -jar target/casscope-1.1.0.jar --config exporter.yaml
```

Внешняя Cassandra нужна только для отдельных лабораторных e2e-сценариев. Они не входят в `mvnw verify`. Python-сценарии рядом с модулем предназначены для выделенного лабораторного кластера и могут временно останавливать его узлы.
