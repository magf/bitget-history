#!/bin/bash

# Конфигурация
DATA_DIR="data"
RAW_PROXY_FILE="$DATA_DIR/proxies_raw.txt"
WORKING_PROXY_FILE="$DATA_DIR/proxies.txt"
CONFIG_FILE="$DATA_DIR/config"

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

# Функция для скачивания списка прокси
download_proxy_list() {
    log "Скачиваем списки прокси..."
    mkdir -p "$DATA_DIR"
    : > "$RAW_PROXY_FILE"
    for proto in 4 5; do
        local url="https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks$proto/data.txt"
        debug "Скачиваем $url..."
        if curl ${MY_PROXY:+-x "$MY_PROXY"} -s -o - "$url" >> "$RAW_PROXY_FILE"; then
            debug "Успешно скачан $url"
            echo '' >> "$RAW_PROXY_FILE"
        else
            log "Ошибка скачивания $url"
        fi
    done

    if [ ! -s "$RAW_PROXY_FILE" ]; then
        err "Файл прокси пуст после скачивания: $RAW_PROXY_FILE"
    fi
}

# Функция для проверки прокси
check_proxies() {
    local timeout="$1"
    debug "Читаем список прокси из $RAW_PROXY_FILE..."
    local proxies=()
    while IFS= read -r line; do
        [ -n "$line" ] && proxies+=("$line")
    done < "$RAW_PROXY_FILE"

    local count=${#proxies[@]}
    if [ "$count" -eq 0 ]; then
        err "Список прокси пуст: $RAW_PROXY_FILE"
    fi
    log "Найдено $count прокси для проверки"

    : > "$WORKING_PROXY_FILE"
    local working_count=0
    for proxy in "${proxies[@]}"; do
        debug "Проверяем прокси $proxy..."
        result=$(curl -s --insecure --connect-timeout "$timeout" --max-time "$((timeout + 2))" -x "$proxy" https://ifconfig.io)
        proxy_ip=${proxy#*://}
        proxy_ip=${proxy_ip%%:*}
        debug "Ожидается '$proxy_ip'. Получено '$result'"
        if [ $? -eq 0 ] && [ "$result" = "$proxy_ip" ]; then
            log "Прокси $proxy работает"
            echo "$proxy" >> "$WORKING_PROXY_FILE"
            working_count=$((working_count + 1))
        else
            debug "Прокси $proxy недоступен или возвращает неверный IP ($result)"
        fi
    done

    if [ "$working_count" -eq 0 ]; then
        err "Не найдено ни одного рабочего прокси"
    fi
    log "Найдено $working_count рабочих прокси, сохранено в $WORKING_PROXY_FILE"
}

# Парсинг аргументов
TIMEOUT=3
DEBUG=""

while [ $# -gt 0 ]; do
    case "$1" in
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --debug) DEBUG=1; shift ;;
        *) err "Неизвестный аргумент: $1" ;;
    esac
done

# Проверка корректности timeout
if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]] || [ "$TIMEOUT" -lt 1 ]; then
    err "Неверное значение timeout: $TIMEOUT (должно быть целое число >= 1)"
fi

# Основной процесс
download_proxy_list
check_proxies "$TIMEOUT"
