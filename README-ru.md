# Bitget History Downloader

Bash-скрипт для скачивания и обработки исторических рыночных данных (сделок и глубины рынка) с публичного API Bitget для криптовалютных пар, таких как BTCUSDT. Скрипт создаёт локальное зеркало, конвертирует данные в CSV и сохраняет в SQLite-базу с защитой от дублирования.

## Возможности

- Поддерживает `trades` (история сделок) и `depth` (снимки книги ордеров) для спотового и фьючерсного рынков.
- Сохраняет `.zip` в `data/offline/`, чтобы не скачивать повторно.
- Конвертирует `.xlsx` (depth) в `.csv` и хранит в `data/csv/`.
- Импортирует данные в SQLite с защитой от дублирования (`trade_id` для trades, `timestamp` для depth).
- Использует SOCKS4/SOCKS5 прокси с настраиваемым резервным прокси.
- Настраивается через `data/config` для чувствительных параметров, таких как прокси.

## Требования

- Linux/Unix-окружение (например, Ubuntu, macOS).
- Зависимости:
  - `curl`: Для скачивания файлов.
  - `unzip`: Для распаковки `.zip`.
  - `sqlite3`: Для работы с базой данных.
  - `ssconvert` (из `gnumeric`): Для конвертации `.xlsx` в `.csv`.
- Установка на Ubuntu/Debian:
  ```bash
  sudo apt-get install curl unzip sqlite3 gnumeric
  ```

## Настройка

Чувствительные параметры, такие как прокси, можно задать в `data/config` (не включён в репозиторий из соображений безопасности). Создайте файл вручную при необходимости.

Пример `data/config`:
```bash
MY_PROXY="socks5://1.2.3.4:1080"
```

Если доступ к `cdn.jsdelivr.net` (для списков прокси) ограничен, укажите локальный прокси в `data/config`. Примеры:
```bash
# HTTP-прокси
MY_PROXY="http://1.2.3.4:3128"
# SOCKS5-прокси
MY_PROXY="socks5://1.2.3.4:1080"
```

## Установка

1. Склонируйте репозиторий:
   ```bash
   git clone https://github.com/magf/bitget-history.git
   cd bitget-history
   ```

2. Сделайте скрипт исполняемым:
   ```bash
   chmod +x download_history.sh
   ```

## Использование

Запустите скрипт с параметрами для пары, типа данных, рынка и диапазона дат:

```bash
./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02
```

### Параметры

- `--pair`: Торговая пара (например, `BTCUSDT`). По умолчанию: `BTCUSDT`.
- `--type`: Тип данных (`trades` или `depth`). Обязательный.
- `--market`: Тип рынка (`spot` или `futures`). По умолчанию: `spot`.
- `--start-date`: Начальная дата (ГГГГ-ММ-ДД). По умолчанию: год назад.
- `--end-date`: Конечная дата (ГГГГ-ММ-ДД). По умолчанию: сегодня.

### Результат

- **ZIP-файлы**: Хранятся в `data/offline/` (например, `data/offline/depth/BTCUSDT/1/20250501.zip`).
- **CSV-файлы**: Хранятся в `data/csv/` (например, `data/csv/depth/BTCUSDT/20250501.csv`).
- **База данных**: SQLite-база в `data/history_<ПАРА>_<РЫНОК>.db` (например, `data/history_BTCUSDT_spot.db`).

### Пример

Скачать данные глубины для BTCUSDT (спот) с 1 по 2 мая 2024:

```bash
./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02
```

Проверить базу:
```bash
sqlite3 data/history_BTCUSDT_spot.db "SELECT COUNT(*) FROM depth;"
```

## Схема базы данных

- **Таблица trades**:
  - `trade_id` (TEXT, PRIMARY KEY): Уникальный ID сделки.
  - `timestamp` (INTEGER): Unix-время (мс).
  - `price` (REAL): Цена сделки.
  - `side` (TEXT): Покупка или продажа.
  - `volume_quote` (REAL): Объём в котируемой валюте.
  - `size_base` (REAL): Объём в базовой валюте.
  - Индекс: `idx_trades_timestamp` на `timestamp`.

- **Таблица depth**:
  - `timestamp` (INTEGER, PRIMARY KEY): Unix-время (мс).
  - `ask_price` (REAL): Лучшая цена продажи.
  - `bid_price` (REAL): Лучшая цена покупки.
  - `ask_volume` (REAL): Объём на продажу.
  - `bid_volume` (REAL): Объём на покупку.
  - Индекс: `idx_depth_timestamp` на `timestamp`.

## Замечания

- Пропускает существующие `.zip` и `.csv` файлы, чтобы не скачивать и не конвертировать повторно.
- Файлы depth одинаковы для спота (`1`) и фьючерсов (`2`) и используются повторно, если уже скачаны.
- Временные файлы хранятся в `/tmp/` и удаляются после обработки.
- Все файлы данных (`data/`) исключены из репозитория через `.gitignore`.

## Лицензия

Проект распространяется под лицензией MIT. См. [LICENSE](LICENSE).

## Как помочь проекту

Открывайте issues или присылайте pull requests на GitHub.

## Автор

Максим Гайдай <maxim.gajdaj@gmail.com>