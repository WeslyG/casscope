# Пример systemd

Файл службы: [examples/systemd/casscope.service](../examples/systemd/casscope.service).

Схема размещения:

```text
/opt/casscope/casscope.jar
/etc/casscope/exporter.yaml
/etc/casscope/casscope.env     # необязательные credentials
/etc/systemd/system/casscope.service
```

## Установка

Создайте системного пользователя и установите скачанный release JAR. Команды рассчитаны на Linux с `useradd`; если пользователь уже существует, повторно создавать его не нужно.

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin casscope
sudo install -d -o root -g casscope -m 0750 /opt/casscope /etc/casscope
sudo install -o root -g casscope -m 0640 casscope-1.0.0.jar /opt/casscope/casscope.jar
sudo install -o root -g casscope -m 0640 examples/exporter.yaml /etc/casscope/exporter.yaml
sudo install -m 0644 examples/systemd/casscope.service /etc/systemd/system/casscope.service
```

Перед запуском отредактируйте cluster/observer, JMX URL и HTTP bind. В `ExecStart` замените `/usr/bin/java` на **фактический путь Java Cassandra**, если он отличается. `JAVA_HOME` сам по себе не меняет абсолютный путь ExecStart. Пути в `/opt` и `/etc` должны читаться пользователем casscope; домашний каталог защищён `ProtectHome=true`.

Если Java Cassandra находится в `/home`, этот пример может скрыть её путь. Скорректируйте `ProtectHome` и права доступа для своего размещения, вместо установки второй Java.

При необходимости создайте `/etc/casscope/casscope.env`:

```text
CASSCOPE_JMX_USER=monitoring-user
CASSCOPE_JMX_PASSWORD=replace-with-your-password
```

Задайте в YAML соответствующие `username_env/password_env`. Environment file читает systemd; храните его с владельцем root и правами `0600`, не добавляйте в Git. Для сложных значений используйте корректное quoting EnvironmentFile, это не shell script.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now casscope
sudo systemctl status casscope
sudo journalctl -u casscope -n 100 --no-pager
curl http://127.0.0.1:9505/metrics
```

Служба не требует root и не управляет Cassandra. Пример намеренно не привязан к конкретному имени Cassandra systemd unit: exporter пережидает её недоступность с backoff. Дополнительные ограничения systemd зависят от версии дистрибутива и должны проверяться на целевой машине.

## Перезапуск и обновление

`Restart=on-failure` перезапускает аварийно завершившийся процесс. **Открытый circuit не завершает процесс**: HTTP остаётся жив, `circuit_open=1`, JMX polling остановлен. После проверки состояния Cassandra восстановление выполняется явно:

```sh
sudo systemctl restart casscope
```

Не ставьте частый restart loop поверх circuit: он обойдёт защиту от повторных запросов к перегруженной Cassandra.

Для обновления остановите Casscope, замените `/opt/casscope/casscope.jar` проверенным JAR и запустите снова. Cassandra перезапускать не нужно. Изменение unit требует daemon-reload; изменение YAML/environment — restart Casscope.

`-Xmx64m` ограничивает heap, а не весь RSS JVM. Значение нужно соотносить с max_endpoints, HTTP-нагрузкой и native memory. Не считайте его общим лимитом процесса.
