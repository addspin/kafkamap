# Конфигурация

Разместите файл конфигурации config.yaml в корневой директории проекта.

```yaml
kafka: 
  broker:
    - "192.168.1.1:9092"
  sasl:
    enabled: true
    securityProtocol: "SASL_PLAINTEXT"
    # Флаг, указывающий на необходимость выполнения рукопожатия SASL перед отправкой запроса (default: true).
    handshake: true 
    # механизм аутентификации (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    mechanism: "PLAIN"
    username: "admin"
    password: "xxx"

  tls:
    enabled: false

  timeout:
    # DialTimeout - максимальное время ожидания при установке TCP-соединения с брокером. Если за это время соединение не установлено, возникнет ошибка. 
    dial: 10s
    # ReadTimeout - максимальное время ожидания при чтении ответа от брокера.
    read: 10s
    # WriteTimeout - максимальное время ожидания при отправке запроса брокеру.
    write: 10s

# Контейнер с kafka где будут выполнятся скрипты ремапинга топиков
container:
  name: "kafka"
  # Список брокеров для перераспределения партиций (за исключением брокера который исключается)
  brokerList: "1,2,3,4,5,8"

```
