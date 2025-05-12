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

# Примеры использования локального прокси для получения списков прокси
# MY_PROXY="http://1.2.3.4:3128"
# MY_PROXY="socks5://1.2.3.4:1080"

[ -f "$CONFIG_FILE" ] && source "$CONFIG_FILE"

# Функция для логирования
log() {
    echo "$(date '+%Y/%m/%d %H:%M:%S') $1"
} ; export -f log

# Функция для отладочного вывода
debug() {
    [ -n "$DEBUG" ] && log "$1" || true
} ; export -f debug

# Функция для вывода ошибки и завершения
err() {
    echo "$(date '+%Y/%m/%d %H:%M:%S') Ошибка: $1" >&2
    exit 1
} ; export -f err

# Функция для проверки типа файла
check_file_type() {
    local file_path="$1"
    local expected_type="$2"
    local file_output

    if [ ! -f "$file_path" ] || [ ! -s "$file_path" ]; then
        debug "Файл $file_path не существует или пуст"
        return 1
    fi

    file_output=$(file "$file_path")
    debug "Проверка типа файла $file_path: $file_output"

    case "$expected_type" in
        zip)
            if echo "$file_output" | grep -q "Zip archive data"; then
                debug "Файл $file_path является корректным Zip-архивом"
                return 0
            else
                log "Файл $file_path не является Zip-архивом: $file_output"
                return 1
            fi
            ;;
        csv)
            if echo "$file_output" | grep -q -E "ASCII text|UTF-8 Unicode text"; then
                debug "Файл $file_path является корректным CSV-файлом"
                return 0
            else
                log "Файл $file_path не является CSV-файлом: $file_output"
                return 1
            fi
            ;;
        *)
            err "Неизвестный тип файла: $expected_type"
            ;;
    esac
}

# Функция для получения рандомного прокси
get_random_proxy() {
    if [ ! -f "$PROXY_FILE" ] || [ ! -s "$PROXY_FILE" ]; then
        err "Файл рабочих прокси отсутствует или пуст: $PROXY_FILE. Запустите check_proxies.sh"
    fi

    debug "Читаем список прокси из $PROXY_FILE..."
    local proxies=()
    while IFS= read -r line; do
        [ -n "$line" ] && proxies+=("$line")
    done < "$PROXY_FILE"

    local count=${#proxies[@]}
    if [ "$count" -eq 0 ]; then
        err "Список прокси пуст: $PROXY_FILE"
    fi
    debug "Найдено $count прокси"

    local index=$((RANDOM % count))
    local proxy=${proxies[$index]}
    log "Выбран прокси $proxy"
    export PROXY="$proxy"
}

# Функция для вычисления коэффициента ротации
calculate_rotation_factor() {
    local days=$(( (END_DATE_EPOCH - START_DATE_EPOCH) / 86400 + 1 ))
    local proxy_count=$(wc -l < "$PROXY_FILE" | tr -d ' ')
    if [ "$proxy_count" -eq 0 ]; then
        err "Список прокси пуст: $PROXY_FILE"
    fi
    local factor=$(( (days + proxy_count - 1) / proxy_count )) # Округление вверх
    debug "Дней: $days, Прокси: $proxy_count, Коэффициент ротации: $factor" >&2
    echo "$factor"
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
#  catastrophically failed to execute
    log "Инициализирована база $db_path"
}

# Функция для импорта CSV в SQLite с защитой от дублей
import_to_db() {
    local db_path="$1"
    local table_name="$2"
    local csv_path="$3"

    if ! check_file_type "$csv_path" csv; then
        log "Пропускаем импорт некорректного CSV-файла $csv_path"
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
DEBUG=""
MAX_RETRIES=3

while [ $# -gt 0 ]; do
    case "$1" in
        --pair) PAIR="$2"; shift 2 ;;
        --start-date) START_DATE="$2"; shift 2 ;;
        --end-date) END_DATE="$2"; shift 2 ;;
        --type) DATA_TYPE="$2"; shift 2 ;;
        --market) MARKET="$2"; shift 2 ;;
        --debug) DEBUG=1; shift ;;
        *) err "Неизвестный аргумент: $1" ;;
    esac
done

if [ -z "$DATA_TYPE" ] || [ "$DATA_TYPE" != "trades" ] && [ "$DATA_TYPE" != "depth" ]; then
    err "Тип данных должен быть 'trades' или 'depth'"
fi

if [ "$MARKET" != "spot" ] && [ "$MARKET" != "futures" ]; then
    err "Рынок должен быть 'spot' или 'futures'"
fi

# Конвертация дат
START_DATE_EPOCH=$(date -d "$START_DATE" +%s)
END_DATE_EPOCH=$(date -d "$END_DATE" +%s)

if [ $START_DATE_EPOCH -gt $END_DATE_EPOCH ]; then
    err "Начальная дата больше конечной"
fi

# Инициализация базы данных
DB_PATH="$DB_DIR/history_${PAIR}_${MARKET}.db"
init_db "$DB_PATH"

# Получение первого прокси
log "Получаем рабочий прокси..."
unset PROXY
get_random_proxy
if [ -z "$PROXY" ]; then
    err "Прокси не выбран (пустая строка)"
fi
log "Используем прокси: $PROXY"

# Вычисление коэффициента ротации
ROTATION_FACTOR=$(calculate_rotation_factor)
DOWNLOAD_COUNTER=0

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

            # Проверка существующего zip
            if [ -f "$LOCAL_ZIP" ]; then
                if check_file_type "$LOCAL_ZIP" zip; then
                    log "Файл $LOCAL_ZIP уже существует, пропускаем скачивание"
                else
                    log "Удаляем некорректный файл $LOCAL_ZIP"
                    rm -f "$LOCAL_ZIP"
                fi
            fi

            # Зеркалирование
            if [ ! -f "$LOCAL_ZIP" ]; then
                log "Скачиваем $BASE_URL/$REMOTE_PATH..."
                curl_output=$(curl --insecure -s -x "$PROXY" -A "$USER_AGENT" --connect-timeout 20 --max-time 60 --write-out "%{http_code}" -o "$LOCAL_ZIP" "$BASE_URL/$REMOTE_PATH" 2>/dev/null)
                DOWNLOAD_COUNTER=$((DOWNLOAD_COUNTER + 1))
                debug "HTTP-код: $curl_output"

                if [ $? -eq 0 ] && [ -s "$LOCAL_ZIP" ] && [ "$curl_output" = "200" ]; then
                    if ! check_file_type "$LOCAL_ZIP" zip; then
                        log "Удаляем некорректный файл $LOCAL_ZIP после скачивания"
                        rm -f "$LOCAL_ZIP"
                    fi
                else
                    log "Не удалось скачать $BASE_URL/$REMOTE_PATH (HTTP-код: $curl_output)"
                    rm -f "$LOCAL_ZIP"
                fi

                # Ротация прокси по коэффициенту
                if [ "$DOWNLOAD_COUNTER" -ge "$ROTATION_FACTOR" ]; then
                    log "Ротация прокси (скачиваний: $DOWNLOAD_COUNTER, коэффициент: $ROTATION_FACTOR)"
                    get_random_proxy
                    log "Используем прокси: $PROXY"
                    DOWNLOAD_COUNTER=0
                fi
            fi

            # Проверка существующего csv
            if [ -f "$CSV_PATH" ]; then
                if check_file_type "$CSV_PATH" csv; then
                    log "CSV $CSV_PATH уже существует, пропускаем распаковку"
                else
                    log "Удаляем некорректный файл $CSV_PATH"
                    rm -f "$CSV_PATH"
                fi
            fi

            # Распаковка и конвертирование
            if [ ! -f "$CSV_PATH" ]; then
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
        if [ -f "$ALT_LOCAL_ZIP" ]; then
            if check_file_type "$ALT_LOCAL_ZIP" zip; then
                log "Файл $ALT_LOCAL_ZIP уже существует, используем его"
                LOCAL_ZIP="$ALT_LOCAL_ZIP"
            else
                log "Удаляем некорректный файл $ALT_LOCAL_ZIP"
                rm -f "$ALT_LOCAL_ZIP"
            fi
        fi

        # Проверка существующего zip
        if [ -f "$LOCAL_ZIP" ]; then
            if check_file_type "$LOCAL_ZIP" zip; then
                log "Файл $LOCAL_ZIP уже существует, пропускаем скачивание"
            else
                log "Удаляем некорректный файл $LOCAL_ZIP"
                rm -f "$LOCAL_ZIP"
            fi
        fi

        # Зеркалирование
        if [ ! -f "$LOCAL_ZIP" ]; then
            log "Скачиваем $BASE_URL/$REMOTE_PATH..."
            curl_output=$(curl --insecure -s -x "$PROXY" -A "$USER_AGENT" --connect-timeout 5 --max-time 30 --write-out "%{http_code}" -o "$LOCAL_ZIP" "$BASE_URL/$REMOTE_PATH" 2>/dev/null)
            DOWNLOAD_COUNTER=$((DOWNLOAD_COUNTER + 1))
            debug "HTTP-код: $curl_output"

            if [ $? -eq 0 ] && [ -s "$LOCAL_ZIP" ] && [ "$curl_output" = "200" ]; then
                if ! check_file_type "$LOCAL_ZIP" zip; then
                    log "Удаляем некорректный файл $LOCAL_ZIP после скачивания"
                    rm -f "$LOCAL_ZIP"
                fi
            else
                log "Не удалось скачать $BASE_URL/$REMOTE_PATH (HTTP-код: $curl_output)"
                rm -f "$LOCAL_ZIP"
            fi

            # Ротация прокси по коэффициенту
            if [ "$DOWNLOAD_COUNTER" -ge "$ROTATION_FACTOR" ]; then
                log "Ротация прокси (скачиваний: $DOWNLOAD_COUNTER, коэффициент: $ROTATION_FACTOR)"
                get_random_proxy
                log "Используем прокси: $PROXY"
                DOWNLOAD_COUNTER=0
            fi
        fi

        # Проверка существующего csv
        if [ -f "$CSV_PATH" ]; then
            if check_file_type "$CSV_PATH" csv; then
                log "CSV $CSV_PATH уже существует, пропускаем конвертацию"
            else
                log "Удаляем некорректный файл $CSV_PATH"
                rm -f "$CSV_PATH"
            fi
        fi

        # Распаковка и конвертирование
        if [ -f "$LOCAL_ZIP" ] && [ ! -f "$CSV_PATH" ]; then
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
