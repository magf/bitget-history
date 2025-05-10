package main

import (
	"context"
	"log"
	"os"

	"github.com/magf/bitget-history/internal/proxy"
	"gopkg.in/yaml.v3"
)

// Config представляет структуру конфигурационного файла.
type Config struct {
	Proxy struct {
		RawFile     string `yaml:"raw_file"`
		WorkingFile string `yaml:"working_file"`
		Fallback    string `yaml:"fallback"`
	} `yaml:"proxy"`
	Database struct {
		Path string `yaml:"path"`
	} `yaml:"database"`
	Downloader struct {
		BaseURL   string `yaml:"base_url"`
		UserAgent string `yaml:"user_agent"`
	} `yaml:"downloader"`
}

func main() {
	// Читаем конфиг
	configFile := "config/config.yaml"
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config %s: %v", configFile, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config %s: %v", configFile, err)
	}

	// Создаём ProxyManager
	pm, err := proxy.NewProxyManager(cfg.Proxy.RawFile, cfg.Proxy.WorkingFile, cfg.Proxy.Fallback)
	if err != nil {
		log.Fatalf("Failed to create proxy manager: %v", err)
	}

	// Запускаем проверку прокси
	log.Println("Ensuring proxies...")
	if err := pm.EnsureProxies(context.Background()); err != nil {
		log.Fatalf("Failed to ensure proxies: %v", err)
	}

	// Получаем рабочие прокси
	proxies, err := pm.GetProxies()
	if err != nil {
		log.Fatalf("Failed to get proxies: %v", err)
	}

	// Логируем результат
	log.Printf("Found %d working proxies:", len(proxies))
	for _, p := range proxies {
		log.Println("  ", p)
	}

	os.Exit(0)
}
