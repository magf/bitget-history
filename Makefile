# Makefile для Bitget History Downloader

GO = go
BINARY = bitget-history
BIN_DIR = bin
CMD_DIR = cmd/$(BINARY)
INSTALL_DIR = /usr/bin
CONFIG_DIR = /etc/bitget-history
DATA_DIR = /var/lib/bitget-history

all: build

build:
	$(GO) mod tidy
	$(GO) build -o $(BIN_DIR)/$(BINARY) ./$(CMD_DIR)

test:
	$(GO) mod tidy
	$(GO) test ./...

clean:
	rm -rf $(BIN_DIR)/*.test $(BIN_DIR)/*.prof *.log

deb:
	dpkg-buildpackage -us -uc -b
	mkdir -p $(BIN_DIR)
	mv ../$(BINARY)_*.deb $(BIN_DIR)/

install:
	install -Dm755 $(BIN_DIR)/$(BINARY) $(INSTALL_DIR)/$(BINARY)
	install -Dm644 config/config.yaml $(CONFIG_DIR)/config.yaml
	mkdir -p $(DATA_DIR)

uninstall:
	rm -f $(INSTALL_DIR)/$(BINARY)
	rm -rf $(CONFIG_DIR)
	rm -rf $(DATA_DIR)

.PHONY: all build test clean deb install uninstall
