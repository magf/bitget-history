#!/bin/bash

# Скрипт для создания структуры Go-проекта в репозитории bitget-history

# Конфигурация
PROJECT_DIR="$(pwd)"
MODULE_NAME="github.com/magf/bitget-history"
DATA_DIR="data"
CONFIG_DIR="config"
BIN_DIR="bin"
CMD_DIR="cmd/bitget-history"
INTERNAL_DIR="internal"
TESTDATA_DIR="testdata"
SCRIPTS_DIR="scripts"
DEBIAN_DIR="pkg/debian"

# Функция для логирования
log() {
    echo "$(date '+%Y/%m/%d %H:%M:%S') $1"
}

# Функция для создания директорий
create_dirs() {
    log "Создаём структуру каталогов..."
    mkdir -p "$DATA_DIR" \
             "$CONFIG_DIR" \
             "$BIN_DIR" \
             "$CMD_DIR" \
             "$INTERNAL_DIR/proxy" \
             "$INTERNAL_DIR/downloader" \
             "$INTERNAL_DIR/db" \
             "$TESTDATA_DIR" \
             "$SCRIPTS_DIR" \
             "$DEBIAN_DIR"
}

# Функция для создания go.mod
create_go_mod() {
    if [ ! -f "go.mod" ]; then
        log "Создаём go.mod..."
        cat > go.mod <<EOF
module $MODULE_NAME

go 1.22

require (
    golang.org/x/net v0.30.0
    gopkg.in/yaml.v3 v3.0.1
    github.com/mattn/go-sqlite3 v1.14.22
)
EOF
    else
        log "go.mod уже существует, пропускаем..."
    fi
}

# Функция для создания пустых Go-файлов
create_go_files() {
    log "Создаём пустые Go-файлы..."
    # cmd/bitget-history/main.go
    if [ ! -f "$CMD_DIR/main.go" ]; then
        cat > "$CMD_DIR/main.go" <<EOF
package main

import (
    "fmt"
    "os"
)

func main() {
    fmt.Println("Bitget History Downloader")
    os.Exit(0)
}
EOF
    fi

    # internal/proxy/proxy.go
    if [ ! -f "$INTERNAL_DIR/proxy/proxy.go" ]; then
        cat > "$INTERNAL_DIR/proxy/proxy.go" <<EOF
package proxy

// Модуль управления прокси
// - Загружает список прокси из data/proxies_raw.txt или скачивает с cdn.jsdelivr.net
// - Проверяет прокси многопоточно (одна goroutine на прокси)
// - Сохраняет рабочие прокси в data/proxies.txt
// - Каждый поток использует фиксированный прокси без ротации
// - Фоллбэк-прокси используется только для загрузки списка прокси
EOF
    fi

    # internal/downloader/downloader.go
    if [ ! -f "$INTERNAL_DIR/downloader/downloader.go" ]; then
        cat > "$INTERNAL_DIR/downloader/downloader.go" <<EOF
package downloader

// TODO: Реализовать модуль загрузки данных
EOF
    fi

    # internal/db/db.go
    if [ ! -f "$INTERNAL_DIR/db/db.go" ]; then
        cat > "$INTERNAL_DIR/db/db.go" <<EOF
package db

// TODO: Реализовать модуль работы с SQLite
EOF
    fi
}

# Функция для создания конфигурационного файла
create_config() {
    if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
        log "Создаём config.yaml..."
        cat > "$CONFIG_DIR/config.yaml" <<EOF
# Конфигурация Bitget History Downloader
proxy:
  raw_file: "data/proxies_raw.txt"
  working_file: "data/proxies.txt"
  fallback: "socks5://1.2.3.4:1080"
database:
  path: "/var/lib/bitget-history/history_%s_%s.db"
downloader:
  base_url: "https://img.bitgetimg.com/online"
  user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
EOF
    else
        log "config.yaml уже существует, пропускаем..."
    fi
}

# Функция для создания Makefile
create_makefile() {
    if [ ! -f "Makefile" ]; then
        log "Создаём Makefile..."
        cat > Makefile <<EOF
# Makefile для Bitget History Downloader

GO = go
BINARY = bitget-history
BIN_DIR = bin
CMD_DIR = cmd/\$(BINARY)
INSTALL_DIR = /usr/bin
CONFIG_DIR = /etc/bitget-history
DATA_DIR = /var/lib/bitget-history

all: build

build:
	\$(GO) build -o \$(BIN_DIR)/\$(BINARY) ./\$(CMD_DIR)

test:
	\$(GO) test ./...

clean:
	rm -rf \$(BIN_DIR)/*.test \$(BIN_DIR)/*.prof *.log

deb:
	dpkg-buildpackage -us -uc -b
	mkdir -p \$(BIN_DIR)
	mv ../\$(BINARY)_*.deb \$(BIN_DIR)/

install:
	install -Dm755 \$(BIN_DIR)/\$(BINARY) \$(INSTALL_DIR)/\$(BINARY)
	install -Dm644 config/config.yaml \$(CONFIG_DIR)/config.yaml
	mkdir -p \$(DATA_DIR)

uninstall:
	rm -f \$(INSTALL_DIR)/\$(BINARY)
	rm -rf \$(CONFIG_DIR)
	rm -rf \$(DATA_DIR)

.PHONY: all build test clean deb install uninstall
EOF
    else
        log "Makefile уже существует, пропускаем..."
    fi
}

# Функция для создания файлов Debian-пакета
create_debian_files() {
    log "Создаём файлы для Debian-пакета..."
    # pkg/debian/control
    if [ ! -f "$DEBIAN_DIR/control" ]; then
        cat > "$DEBIAN_DIR/control" <<EOF
Package: bitget-history
Version: 0.2.0
Architecture: amd64
Maintainer: Maxim Gajdaj <maxim.gajdaj@gmail.com>
Depends: curl, unzip, sqlite3, gnumeric
Section: utils
Priority: optional
Homepage: https://github.com/magf/bitget-history
Description: Bitget History Downloader
 A tool for downloading and processing historical market data from Bitget's public API.
 Supports trades and depth data for spot and futures markets, with SQLite storage.
EOF
    fi

    # pkg/debian/rules
    if [ ! -f "$DEBIAN_DIR/rules" ]; then
        cat > "$DEBIAN_DIR/rules" <<EOF
#!/usr/bin/make -f
%:
	dh \$@

override_dh_auto_build:
	go build -o bitget-history ./cmd/bitget-history

override_dh_auto_install:
	install -Dm755 bitget-history \$(DESTDIR)/usr/bin/bitget-history
	install -Dm644 config/config.yaml \$(DESTDIR)/etc/bitget-history/config.yaml
	mkdir -p \$(DESTDIR)/var/lib/bitget-history
EOF
        chmod +x "$DEBIAN_DIR/rules"
    fi

    # pkg/debian/compat
    if [ ! -f "$DEBIAN_DIR/compat" ]; then
        cat > "$DEBIAN_DIR/compat" <<EOF
12
EOF
    fi

    # pkg/debian/copyright
    if [ ! -f "$DEBIAN_DIR/copyright" ]; then
        cat > "$DEBIAN_DIR/copyright" <<EOF
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: bitget-history
Source: https://github.com/magf/bitget-history

Files: *
Copyright: 2025 Maxim Gajdaj <maxim.gajdaj@gmail.com>
License: MIT
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 .
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 .
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
EOF
    fi

    # pkg/debian/changelog
    if [ ! -f "$DEBIAN_DIR/changelog" ]; then
        cat > "$DEBIAN_DIR/changelog" <<EOF
bitget-history (0.2.0-1) unstable; urgency=medium

  * Initial Go-based release
  * Supports downloading trades and depth data
  * Stores data in SQLite with deduplication
  * Uses fixed SOCKS4/SOCKS5 proxies per thread

 -- Maxim Gajdaj <maxim.gajdaj@gmail.com>  Sat, 10 May 2025 12:00:00 +0000
EOF
    fi

    # pkg/debian/bitget-history.install
    if [ ! -f "$DEBIAN_DIR/bitget-history.install" ]; then
        cat > "$DEBIAN_DIR/bitget-history.install" <<EOF
bitget-history /usr/bin/
config/config.yaml /etc/bitget-history/
EOF
    fi
}

# Функция для обновления .gitignore
update_gitignore() {
    if [ ! -f ".gitignore" ]; then
        log "Создаём .gitignore..."
        cat > .gitignore <<EOF
# Go
bin/
*.log
*.test
*.prof

# Data files
data/

# Temporary files
/tmp/bitget_*

# Configuration
config/config.yaml

# Debian build
debian/files
debian/*.debhelper*
debian/*.substvars
debian/bitget-history/
*.deb
EOF
    else
        log "Обновляем .gitignore..."
        grep -q "bin/" .gitignore || echo "bin/" >> .gitignore
        grep -q "*.log" .gitignore || echo "*.log" >> .gitignore
        grep -q "*.test" .gitignore || echo "*.test" >> .gitignore
        grep -q "*.prof" .gitignore || echo "*.prof" >> .gitignore
        grep -q "data/" .gitignore || echo "data/" >> .gitignore
        grep -q "/tmp/bitget_*" .gitignore || echo "/tmp/bitget_*" >> .gitignore
        grep -q "config/config.yaml" .gitignore || echo "config/config.yaml" >> .gitignore
        grep -q "debian/" .gitignore || echo "debian/" >> .gitignore
        grep -q "*.deb" .gitignore || echo "*.deb" >> .gitignore
    fi
}

# Основной процесс
log "Инициализация Go-проекта в $PROJECT_DIR..."
create_dirs
create_go_mod
create_go_files
create_config
create_makefile
create_debian_files
update_gitignore
log "Структура проекта создана успешно!"
log "Следующие шаги:"
log "1. Проверьте go.mod и добавьте актуальные версии зависимостей."
log "2. Запустите 'go mod tidy' для очистки зависимостей."
log "3. Скомпилируйте: 'make build'"
log "4. Создайте Debian-пакет: 'make deb'"
log "5. Начните разработку с internal/proxy/proxy.go."
