#!/bin/bash

# Конфигурация
BASE_URL="https://img.bitgetimg.com/online"
DATA_DIR="data"
DB_DIR="$DATA_DIR"
CSV_DIR="$DATA_DIR/csv"
OFFLINE_DIR="$DATA_DIR/offline"
PROXY_FILE="$DATA_DIR/proxies.txt"
CONFIG_FILE="$DATA_DIR/config"
USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

[ -f "$CONFIG_FILE" ] && source "$CONFIG_FILE"

# Функция для логирования
log() {
    echo "$(date '+%Y/%m/%d %H:%M:%S') $1"
}

# Функция для скачивания списка прокси
download_proxy_list() {
    log "Скачиваем списки прокси..."
    mkdir -p "$DATA_DIR"
    for proto in 4 5; do
        local url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks$proto/data.txt"
        log "Скачиваем $url..."
        if curl -x "$MY_PROXY" -s -o - "$url" >> "$PROXY_FILE"; then
            log "Успешно скачан $url"
            echo '' >> "$PROXY_FILE"
        else
            log "Ошибка скачивания $url"
        fi
    done

    if [ ! -s "$PROXY_FILE" ]; then
        log "Ошибка: Файл прокси пуст после скачивания"
        return 1
    fi
    return 0
}

# Функция для получения рандомного прокси
get_random_proxy() {
    [ -f "$PROXY_FILE" ] || { download_proxy_list || { log "Ошибка: Не удалось скачать список прокси"; return 1; } }

    log "Читаем список прокси из $PROXY_FILE..."
    local proxies=()
    while IFS= read -r line; do
        [ -n "$line" ] && proxies+=("$line")
    done < "$PROXY_FILE"

    local count=${#proxies[@]}
    if [ "$count" -eq 0 ]; then
        log "Ошибка: Список прокси пуст после чтения"
        return 1
    fi
    log "Найдено $count прокси"

    for ((i=1; i<=30; i++)); do
        local index=$((RANDOM % count))
        local proxy=${proxies[$index]}
        log "Попытка $i: Проверяем прокси $proxy..."
        result=$(curl -s --insecure --connect-timeout 3 -x "$proxy" https://ifconfig.io)
        proxy_ip=${proxy#*://}
        proxy_ip=${proxy_ip%%:*}
        log "Ожидается '$proxy_ip'. Получено '$result'"
        if [ $? -eq 0 ] && [ "$result" = "$proxy_ip" ]; then
            log "Прокси $proxy доступен, возвращает IP $result"
            export PROXY="$proxy"
            return 0
        fi
        log "Прокси $proxy недоступен или возвращает неверный IP ($result)"
    done

    log "Ошибка: Не удалось найти доступный прокси после 30 попыток"
    return 1
}

# Функция для инициализации SQLite базы
init_db() {
    local db_path="$1"
    sqlite3 "$db_path" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    timestamp INTEGER,
    price REAL,
    side TEXT,
    volume_quote REAL,
    size_base REAL
);
CREATE TABLE IF NOT EXISTS depth (
    timestamp INTEGER PRIMARY KEY,
    ask_price REAL,
    bid_price REAL,
    ask_volume REAL,
    bid_volume REAL
);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_depth_timestamp ON depth(timestamp);
EOF
    log "Инициализирована база $db_path"
}

# Функция для импорта CSV в SQLite с защитой от дублей
import_to_db() {
    local db_path="$1"
    local table_name="$2"
    local csv_path="$3"

    if [ ! -f "$csv_path" ] || [ ! -s "$csv_path" ]; then
        log "Ошибка: CSV-файл $csv_path не существует или пуст"
        return 1
    fi

    if [ "$table_name" = "trades" ]; then
        sqlite3 -separator ',' "$db_path" <<EOF
CREATE TEMP TABLE temp_trades (
    trade_id TEXT,
    timestamp INTEGER,
    price REAL,
    side TEXT,
    volume_quote REAL,
    size_base REAL
);
.mode csv
.import --skip 1 "$csv_path" temp_trades
INSERT OR IGNORE INTO trades SELECT * FROM temp_trades WHERE trade_id NOT IN (SELECT trade_id FROM trades);
DROP TABLE temp_trades;
EOF
    elif [ "$table_name" = "depth" ]; then
        sqlite3 -separator ',' "$db_path" <<EOF
CREATE TEMP TABLE temp_depth (
    timestamp INTEGER,
    ask_price REAL,
    bid_price REAL,
    ask_volume REAL,
    bid_volume REAL
);
.mode csv
.import --skip 1 "$csv_path" temp_depth
INSERT OR IGNORE INTO depth SELECT * FROM temp_depth WHERE timestamp NOT IN (SELECT timestamp FROM depth);
DROP TABLE temp_depth;
EOF
    fi
    log "Импортирован $csv_path в таблицу $table_name"
}

# Парсинг аргументов
PAIR="BTCUSDT"
START_DATE=$(date -d "1 year ago" +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)
DATA_TYPE=""
MARKET="spot"

while [ $# -gt 0 ]; do
    case "$1" in
        --pair) PAIR="$2"; shift 2 ;;
        --start-date) START_DATE="$2"; shift 2 ;;
        --end-date) END_DATE="$2"; shift 2 ;;
        --type) DATA_TYPE="$2"; shift 2 ;;
        --market) MARKET="$2"; shift 2 ;;
        *) echo "Неизвестный аргумент: $1"; exit 1 ;;
    esac
done

if [ -z "$DATA_TYPE" ] || [ "$DATA_TYPE" != "trades" ] && [ "$DATA_TYPE" != "depth" ]; then
    echo "Ошибка: --type должен быть 'trades' или 'depth'"
    exit 1
fi

if [ "$MARKET" != "spot" ] && [ "$MARKET" != "futures" ]; then
    echo "Ошибка: --market должен быть 'spot' или 'futures'"
    exit 1
fi

# Конвертация дат
START_DATE_EPOCH=$(date -d "$START_DATE" +%s)
END_DATE_EPOCH=$(date -d "$END_DATE" +%s)

if [ $START_DATE_EPOCH -gt $END_DATE_EPOCH ]; then
    log "Неверный интервал. Начальная дата больше конечной"
    exit 1
fi

# Инициализация базы данных
DB_PATH="$DB_DIR/history_${PAIR}_${MARKET}.db"
init_db "$DB_PATH"

# Получение прокси
log "Получаем рабочий прокси..."
unset PROXY
get_random_proxy
if [ $? -ne 0 ]; then
    log "Критическая ошибка: Не удалось получить прокси"
    exit 1
fi
log "Прокси $PROXY"
if [ -z "$PROXY" ]; then
    log "Критическая ошибка: Прокси не выбран (пустая строка)"
    exit 1
fi
log "Используем прокси: $PROXY"

# Основной цикл по датам
CURRENT_DATE=$START_DATE_EPOCH
while [ $CURRENT_DATE -le $END_DATE_EPOCH ]; do
    DATE_STR=$(date -d "@$CURRENT_DATE" +%Y%m%d)
    TMP_DIR="/tmp/bitget_$DATE_STR_$$"

    if [ "$DATA_TYPE" = "trades" ]; then
        MARKET_CODE=$([ "$MARKET" = "spot" ] && echo "SPBL" || echo "UMCBL")
        NUM=1
        while true; do
            REMOTE_PATH="trades/${MARKET_CODE}/${PAIR}/${DATE_STR}_${NUM}.zip"
            LOCAL_ZIP="$OFFLINE_DIR/$REMOTE_PATH"
            CSV_PATH="$CSV_DIR/trades/${MARKET_CODE}/${PAIR}/${DATE_STR}_${NUM}.csv"
            mkdir -p "$(dirname "$LOCAL_ZIP")" "$(dirname "$CSV_PATH")"

            # Зеркалирование
            if [ -f "$LOCAL_ZIP" ] && [ -s "$LOCAL_ZIP" ]; then
                log "Файл $LOCAL_ZIP уже существует, пропускаем скачивание"
            else
                log "Скачиваем $BASE_URL/$REMOTE_PATH..."
                curl --insecure -s -x "$PROXY" -A "$USER_AGENT" -o "$LOCAL_ZIP" "$BASE_URL/$REMOTE_PATH"
                if [ $? -ne 0 ] || [ ! -s "$LOCAL_ZIP" ]; then
                    log "Ошибка скачивания $BASE_URL/$REMOTE_PATH (вероятно, файл не существует), прерываем для этой даты"
                    rm -f "$LOCAL_ZIP"
                    break
                fi
            fi

            # Распаковка и конвертирование
            if [ -f "$CSV_PATH" ] && [ -s "$CSV_PATH" ]; then
                log "CSV $CSV_PATH уже существует, пропускаем распаковку"
            else
                log "Распаковываем $LOCAL_ZIP в $TMP_DIR..."
                mkdir -p "$TMP_DIR"
                unzip -o "$LOCAL_ZIP" -d "$TMP_DIR" || { log "Ошибка распаковки $LOCAL_ZIP"; rm -rf "$TMP_DIR"; break; }

                log "Ищем .csv файлы в $TMP_DIR..."
                find "$TMP_DIR" -name "*.csv" | while read -r csv_file; do
                    mv "$csv_file" "$CSV_PATH"
                    import_to_db "$DB_PATH" "trades" "$CSV_PATH"
                done
                rm -rf "$TMP_DIR"
            fi

            NUM=$((NUM + 1))
        done
    else
        MARKET_CODE=$([ "$MARKET" = "spot" ] && echo "1" || echo "2")
        REMOTE_PATH="depth/${PAIR}/${MARKET_CODE}/${DATE_STR}.zip"
        LOCAL_ZIP="$OFFLINE_DIR/$REMOTE_PATH"
        CSV_PATH="$CSV_DIR/depth/${PAIR}/${DATE_STR}.csv"
        mkdir -p "$(dirname "$LOCAL_ZIP")" "$(dirname "$CSV_PATH")"

        # Проверяем альтернативную папку (1 или 2)
        ALT_MARKET_CODE=$([ "$MARKET_CODE" = "1" ] && echo "2" || echo "1")
        ALT_LOCAL_ZIP="$OFFLINE_DIR/depth/${PAIR}/${ALT_MARKET_CODE}/${DATE_STR}.zip"
        if [ -f "$ALT_LOCAL_ZIP" ] && [ -s "$ALT_LOCAL_ZIP" ]; then
            log "Файл $ALT_LOCAL_ZIP уже существует, используем его"
            LOCAL_ZIP="$ALT_LOCAL_ZIP"
        fi

        # Зеркалирование
        if [ -f "$LOCAL_ZIP" ] && [ -s "$LOCAL_ZIP" ]; then
            log "Файл $LOCAL_ZIP уже существует, пропускаем скачивание"
        else
            log "Скачиваем $BASE_URL/$REMOTE_PATH..."
            curl --insecure -s -x "$PROXY" -A "$USER_AGENT" -o "$LOCAL_ZIP" "$BASE_URL/$REMOTE_PATH"
            if [ $? -ne 0 ] || [ ! -s "$LOCAL_ZIP" ]; then
                log "Ошибка скачивания $BASE_URL/$REMOTE_PATH или файл пуст, пропускаем дату"
                rm -f "$LOCAL_ZIP"
                continue
            fi
        fi

        # Распаковка и конвертирование
        if [ -f "$CSV_PATH" ] && [ -s "$CSV_PATH" ]; then
            log "CSV $CSV_PATH уже существует, пропускаем конвертацию"
        else
            log "Распаковываем $LOCAL_ZIP в $TMP_DIR..."
            mkdir -p "$TMP_DIR"
            unzip -o "$LOCAL_ZIP" -d "$TMP_DIR" || { log "Ошибка распаковки $LOCAL_ZIP"; rm -rf "$TMP_DIR"; continue; }

            log "Ищем .xlsx файлы в $TMP_DIR..."
            find "$TMP_DIR" -name "*.xlsx" | while read -r xlsx_file; do
                log "Конвертируем $xlsx_file в $CSV_PATH..."
                ssconvert "$xlsx_file" "$CSV_PATH" 2>/dev/null || { log "Ошибка конвертации $xlsx_file"; continue; }
                import_to_db "$DB_PATH" "depth" "$CSV_PATH"
            done
            rm -rf "$TMP_DIR"
        fi
    fi

    CURRENT_DATE=$((CURRENT_DATE + 86400)) # +1 день
done

log "Завершено. Данные сохранены в $DB_PATH"
