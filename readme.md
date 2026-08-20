# jdbcBalancer

## High-Performance PostgreSQL Proxy & Load Balancer

Легковесный, высокопроизводительный прокси-сервер и балансировщик нагрузки
для **PostgreSQL**. Разработан для прозрачной аутентификации legacy/JDBC
клиентов с конвертацией Cleartext ↔ SCRAM-SHA-256, поддержки
отказоустойчивости (Failover). Имеет защиту от аномалий пулов соединений
через **IP-Hash Sticky Sessions**.

---

## Что делает программа?

`jdbcBalancer` решает ключевые проблемы взаимодействия клиентских приложений и пулов
соединений с кластерами PostgreSQL:

1. **Прозрачная трансляция аутентификации (Cleartext ↔ SCRAM-SHA-256):**  
Позволяет клиентам, не поддерживающим современный протокол SCRAM-SHA-256
(или использующим упрощенную авторизацию), безопасно подключаться к
защищенным базам данных PostgreSQL 10+.


2. **Работа с несколькими базами данных:**  
В конфигурационном файле содержится как информация для настройки самого прокси,
так и параметры баз данных. `jdbcBalancer` позволяет держать соединения с базами
данных с разными логинами и паролями, что иногда требуется для безопасности.
При этом реализована возможность указать какой из способов балансировки
требуется использовать:
   1. `ip-hash` поддерживает для каждого уникального ip автоматически выбранную базу
   данных, исключая ошибки TCP-коннекта для пулов соединений (HikariCP, c3p0, Tomcat,
   DBeaver, DataGrip). Этот алгоритм гарантирует, что все соединения одного клиента
   привязываются к одной ноде, исключая ошибки по типу`SQL Error [3F000]`
   и рассинхронизации транзакций.
   2. `round-robin` простое разделение коннектов по приоритетам с чередованием баз
   данных между соединениями.


3. **Отказоустойчивость (Automatic Failover):**  
Непрерывный фоновый опрос состояния нод (Health Check) и автоматическое
переключение трафика на резервные базы данных (`priority: 2`) при падении
основных (число приоритетов может быть больше).


4. **Максимальная производительность (Zero-Copy):**  
Использование пула буферов `sync.Pool` (64 KB) и флагов `TCP_NODELAY` для исключения
задержек передачи сетевых пакетов.

---

## Конфигурация (`config.yaml`)

В графе `server` указываются параметры для прокси, ниже в `databases` указываются
данные от баз данных, с которыми он будет работать. Баз данных с `priority: 1`
может быть несколько.

```yaml
server:
port: 8079
login: "jdbcBalancer"
pass: "jdbc123proxy789balancer456pass"
database: "jdbcProxy"
algorithm: "ip-hash"     # "ip-hash" (рекомендуется) или "round-robin"
timeout_sec: 5

databases:
# Основной сервер (Priority 1) — трафик идет сюда
- url: "jdbc:postgresql://10.0.0.1:5432/app_db"
  login: "db_user_1"
  pass: "db_password_1"
  priority: 1

# Резервный сервер (Priority 2) — активируется ТОЛЬКО при падении Priority 1
- url: "jdbc:postgresql://10.0.0.2:5432/app_db"
  login: "db_user_2"
  pass: "db_password_2"
  priority: 2
```

---

## Сборка и запуск

Требуется **Go 1.21+**

```bash
# Сборка бинарного файла
go build -o jdbcBalancer main.go

# Запуск в текстовом режиме логирования
./jdbcBalancer -config=config.yaml -log-level=info

# Запуск в режиме структурированного JSON-логирования (для Kubernetes / Docker)
./jdbcBalancer -config=config.yaml -json-log=true -log-level=info
```

Тесты

```bash
# Тест с проверкой на гонку данных
go test -v -race ./proxy/...

# Тест с покрытием
go test -coverprofile coverage.out ./proxy/...

# Вывод подробного покрытия кода
go tool cover -func coverage.out
go tool cover -html coverage.out -o coverage.html
```

---

## Как программа работает внутри

### Схема обработки соединения

```text
[ Client (DBeaver / HikariCP) ]
               │
               ▼  TCP Port :8079
┌──────────────────────────────────────────────────────────────┐
│                        jdbcBalancer                          │
│                                                              │
│  1. ReadStartupPacket                                        │
│     ├── Ответ 'N' на SSLRequest (80877103)                   │
│     ├── Ответ 'N' на GSSENCRequest (80877104)                │
│     └── Парсинг параметров: user, database                   │
│                                                              │
│  2. Router (SelectDatabase)                                  │
│     ├── Фильтрация доступных нод (Health Checker)            │
│     ├── Группировка по приоритетам (Priority 1 -> 2)         │
│     └── Выбор ноды: IP-Hash(clientIP) или Round-Robin        │
│                                                              │
│  3. In-Process Auth Bridge (AuthenticateAndBridge)           │
│     ├── Отправка StartupMessage на целевую БД                │
│     ├── Получение AuthenticationSASL от PostgreSQL           │
│     ├── Запрос CleartextPassword у клиента                   │
│     ├── Валидация пароля клиента (pass из config.yaml)       │
│     └── Выполнение RFC 5802 SCRAM-SHA-256 с PostgreSQL       │
│                                                              │
│  4. Bidirectional Proxying                                   │
│     └── Zero-alloc стриминг пакетов через sync.Pool          │
└──────────────────────────────┬───────────────────────────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
       [ PostgreSQL Primary ]      [ PostgreSQL Standby ]
        (Priority 1 - Active)       (Priority 2 - Failover)
```

### Детали реализации компонентов:

#### `protocol.go` Сетевой парсер:

* При первом подключении драйверы PostgreSQL отправляют служебные пакеты
согласования шифрования (SSL или GSSAPI). Прокси перехватывает их, корректно
отвечает байтом 'N' (отказ от SSL) и переходит к чтению основного пакета
StartupMessage с параметрами сессии.

#### `router.go` Маршрутизатор и Health Check:

* Каждые 10 секунд в фоне выполняет легкий handshake к каждой БД.
Если нода не отвечает за 2 секунды — помечает ее как Unhealthy.
*  Метод SelectDatabase(clientAddr) отсекает динамический порт клиента через
`net.SplitHostPort`, вычисляет хеш `fnv.New32a()` от IP-адреса и детерминированно
направляет запрос на одну из живых нод.

#### `auth_bridge.go` & `scram.go` Мост авторизации:

* Клиенту отправляется AuthenticationCleartextPassword.
* Прокси проверяет пароль клиента. Если он неверен — отправляет стандартный
ErrorResponse (код 28P01) и закрывает сокет.
* Если пароль верен — прокси инициирует SCRAM-SHA-256 диалог с бэкендом
(ClientFirstMessage → ServerFirstMessage → ClientFinalMessage → ServerFinalMessage
→ AuthenticationOk).

#### `server.go` Транспортный уровень:

* После успешной авторизации соединение переводится в режим сквозного
двунаправленного копирования (`io.CopyBuffer`) с использованием пула
буферов `sync.Pool`.
