package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/magf/bitget-history/internal/downloader"
	"github.com/magf/bitget-history/internal/proxymanager"
	"golang.org/x/net/proxy"
	"gopkg.in/yaml.v3"

	_ "github.com/bdandy/go-socks4" // Поддержка SOCKS4
)

// Config представляет структуру конфигурационного файла.
type Config struct {
	Proxy struct {
		RawFile     string `yaml:"raw_file"`
		WorkingFile string `yaml:"working_file"`
		Fallback    string `yaml:"fallback"`
		Username    string `yaml:"username"`
		Password    string `yaml:"password"`
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
	// Парсим флаги
	helpFlag := flag.Bool("help", false, "Show help message")
	pairFlag := flag.String("pair", "BTCUSDT", "Trading pair (e.g., BTCUSDT)")
	typeFlag := flag.String("type", "", "Data type: trades or depth")
	marketFlag := flag.String("market", "spot", "Market type: spot or futures")
	startFlag := flag.String("start", "", "Start date (YYYY-MM-DD, default: 1 year ago)")
	endFlag := flag.String("end", "", "End date (YYYY-MM-DD, default: today)")
	timeoutFlag := flag.Int("timeout", 3, "Proxy check timeout in seconds")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")

	// Короткие флаги
	flag.BoolVar(helpFlag, "h", false, "Show help message (short)")
	flag.StringVar(pairFlag, "p", "BTCUSDT", "Trading pair (short)")
	flag.StringVar(typeFlag, "t", "", "Data type (short)")
	flag.StringVar(marketFlag, "m", "spot", "Market type (short)")
	flag.StringVar(startFlag, "s", "", "Start date (short)")
	flag.StringVar(endFlag, "e", "", "End date (short)")
	flag.IntVar(timeoutFlag, "T", 3, "Proxy check timeout in seconds (short)")
	flag.BoolVar(debugFlag, "d", false, "Enable debug logging (short)")

	flag.Parse()

	// Выводим справку, если указан --help или нет параметров
	if *helpFlag || len(os.Args) == 1 {
		printHelp()
		os.Exit(0)
	}

	// Проверяем обязательный флаг --type
	if *typeFlag == "" {
		log.Fatal("Error: --type (-t) is required (trades or depth)")
	}
	if *typeFlag != "trades" && *typeFlag != "depth" {
		log.Fatalf("Error: invalid --type value: %s (must be trades or depth)", *typeFlag)
	}

	// Проверяем market
	if *marketFlag != "spot" && *marketFlag != "futures" {
		log.Fatalf("Error: invalid --market value: %s (must be spot or futures)", *marketFlag)
	}

	// Устанавливаем даты
	endDate := time.Now()
	if *endFlag != "" {
		var err error
		endDate, err = time.Parse("2006-01-02", *endFlag)
		if err != nil {
			log.Fatalf("Error: invalid --end format: %v", err)
		}
	}
	startDate := endDate.AddDate(-1, 0, 0)
	if *startFlag != "" {
		var err error
		startDate, err = time.Parse("2006-01-02", *startFlag)
		if err != nil {
			log.Fatalf("Error: invalid --start format: %v", err)
		}
	}

	// Проверяем даты
	if startDate.After(endDate) {
		log.Fatal("Error: start date is after end date")
	}

	// Включаем дебаг-логирование
	if *debugFlag {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	// Читаем конфиг
	configFile := filepath.Join("config", "config.yaml")
	configOverrideFile := filepath.Join("config", "config-override.yaml")
	var cfg Config

	// Читаем основной конфиг
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config %s: %v", configFile, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config %s: %v", configFile, err)
	}

	// Читаем переопределение, если есть
	if _, err := os.Stat(configOverrideFile); err == nil {
		overrideData, err := os.ReadFile(configOverrideFile)
		if err != nil {
			log.Fatalf("Failed to read override config %s: %v", configOverrideFile, err)
		}
		if err := yaml.Unmarshal(overrideData, &cfg); err != nil {
			log.Fatalf("Failed to parse override config %s: %v", configOverrideFile, err)
		}
	}

	// Создаём ProxyManager
	timeout := time.Duration(*timeoutFlag) * time.Second
	pm, err := proxymanager.NewProxyManager(cfg.Proxy.RawFile, cfg.Proxy.WorkingFile, cfg.Proxy.Fallback, cfg.Proxy.Username, cfg.Proxy.Password, timeout)
	if err != nil {
		log.Fatalf("Failed to create proxy manager: %v", err)
	}

	// Проверяем прокси
	log.Println("Ensuring proxies...")
	if err := pm.EnsureProxies(context.Background()); err != nil {
		log.Fatalf("Failed to ensure proxies: %v", err)
	}

	// Получаем рабочие прокси
	proxies, err := pm.GetProxies()
	if err != nil {
		log.Fatalf("Failed to get proxies: %v", err)
	}
	log.Printf("Found %d working proxies", len(proxies))

	// Создаём Downloader
	outputDir := "/var/lib/bitget-history/offline"
	dl, err := downloader.NewDownloader(cfg.Downloader.BaseURL, cfg.Downloader.UserAgent, outputDir, pm)
	if err != nil {
		log.Fatalf("Failed to create downloader: %v", err)
	}

	// Генерируем URL-ы
	urls, err := generateURLs(cfg.Downloader.BaseURL, *marketFlag, *pairFlag, *typeFlag, startDate, endDate, *debugFlag, proxies, cfg.Downloader.UserAgent)
	if err != nil {
		log.Fatalf("Failed to generate URLs: %v", err)
	}

	// Запускаем загрузку
	log.Println("Downloading files...")
	if err := dl.DownloadFiles(context.Background(), urls); err != nil {
		log.Fatalf("Failed to download files: %v", err)
	}

	log.Println("Download completed successfully")
	os.Exit(0)
}

// generateURLs генерирует список URL-ов на основе параметров.
func generateURLs(baseURL, market, pair, dataType string, startDate, endDate time.Time, debug bool, proxies []string, userAgent string) ([]string, error) {
	var urls []string
	days := int(endDate.Sub(startDate).Hours()/24) + 1

	if dataType == "trades" {
		marketCode := "SPBL"
		if market == "futures" {
			marketCode = "UMCBL"
		}
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
			dateStr := d.Format("20060102")
			// Пробуем файлы _001, _002, ... пока код < 400
			for num := 1; num <= 999; num++ {
				path := fmt.Sprintf("trades/%s/%s/%s_%03d.zip", marketCode, pair, dateStr, num)
				url := fmt.Sprintf("%s/%s", baseURL, path)
				// Проверяем существование файла через HEAD
				exists, err := checkFileExists(url, proxies, userAgent, debug)
				if err != nil {
					if debug {
						log.Printf("Error checking %s: %v", url, err)
					}
					continue
				}
				if !exists {
					if debug {
						log.Printf("Stopping at %s: file does not exist", url)
					}
					break
				}
				if debug {
					log.Printf("Generated URL: %s", url)
				}
				urls = append(urls, url)
			}
		}
	} else { // depth
		marketCodes := []string{"1", "2"} // Проверяем оба кода
		for _, marketCode := range marketCodes {
			for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
				path := fmt.Sprintf("depth/%s/%s/%s.zip", pair, marketCode, d.Format("20060102"))
				url := fmt.Sprintf("%s/%s", baseURL, path)
				// Проверяем существование файла через HEAD
				exists, err := checkFileExists(url, proxies, userAgent, debug)
				if err != nil {
					if debug {
						log.Printf("Error checking %s: %v", url, err)
					}
					continue
				}
				if !exists {
					if debug {
						log.Printf("Skipping %s: file does not exist", url)
					}
					continue
				}
				if debug {
					log.Printf("Generated URL: %s", url)
				}
				urls = append(urls, url)
			}
		}
	}

	// Вычисляем коэффициент ротации прокси
	proxyCount, err := readProxyCount()
	if err != nil {
		return nil, fmt.Errorf("failed to read proxy count: %w", err)
	}
	rotationFactor := int(math.Ceil(float64(days) / float64(proxyCount)))
	if debug {
		log.Printf("Days: %d, Proxies: %d, Rotation factor: %d", days, proxyCount, rotationFactor)
	}

	return urls, nil
}

// checkFileExists проверяет существование файла через HEAD-запрос.
func checkFileExists(urlStr string, proxies []string, userAgent string, debug bool) (bool, error) {
	maxAttempts := 3
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Выбираем случайный прокси
		proxyIndex := rand.Intn(len(proxies))
		proxyURLStr := proxies[proxyIndex]
		proxyURL, err := url.Parse(proxyURLStr)
		if err != nil {
			if debug {
				log.Printf("Invalid proxy URL %s: %v", proxyURLStr, err)
			}
			lastErr = err
			continue
		}

		dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
		if err != nil {
			if debug {
				log.Printf("Failed to create proxy %s: %v", proxyURLStr, err)
			}
			lastErr = err
			continue
		}

		client := &http.Client{
			Transport: &http.Transport{
				Dial: dialer.Dial,
			},
			Timeout: 15 * time.Second,
		}

		req, err := http.NewRequest("HEAD", urlStr, nil)
		if err != nil {
			if debug {
				log.Printf("Failed to create request for %s: %v", urlStr, err)
			}
			lastErr = err
			continue
		}
		req.Header.Set("User-Agent", userAgent)

		resp, err := client.Do(req)
		if err != nil {
			if debug {
				log.Printf("Attempt %d: Failed to HEAD %s with proxy %s: %v", attempt, urlStr, proxyURLStr, err)
			}
			lastErr = err
			continue
		}
		resp.Body.Close()

		if debug {
			log.Printf("Checked %s with proxy %s: status %d", urlStr, proxyURLStr, resp.StatusCode)
		}
		// Явно считаем файл существующим только при 200-399, иначе прерываем при 400+
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			return true, nil
		} else if resp.StatusCode >= 400 {
			return false, nil
		}
	}
	if debug {
		log.Printf("File %s skipped after %d attempts due to error: %v", urlStr, maxAttempts, lastErr)
	}
	return false, fmt.Errorf("failed to check %s after %d attempts: %v", urlStr, maxAttempts, lastErr)
}

// readProxyCount читает количество прокси из файла.
func readProxyCount() (int, error) {
	file, err := os.Open("data/proxies.txt")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			count++
		}
	}
	if count == 0 {
		return 0, fmt.Errorf("proxy list is empty")
	}
	return count, scanner.Err()
}

// printHelp выводит справку.
func printHelp() {
	fmt.Println("Bitget History Downloader")
	fmt.Println("Usage: bitget-history [options]")
	fmt.Println("Options:")
	fmt.Println("  --help, -h          Show this help message")
	fmt.Println("  --pair, -p string   Trading pair (e.g., BTCUSDT) (default: BTCUSDT)")
	fmt.Println("  --type, -t string   Data type: trades or depth (required)")
	fmt.Println("  --market, -m string Market type: spot or futures (default: spot)")
	fmt.Println("  --start, -s string  Start date (YYYY-MM-DD, default: 1 year ago)")
	fmt.Println("  --end, -e string    End date (YYYY-MM-DD, default: today)")
	fmt.Println("  --timeout, -T int   Proxy check timeout in seconds (default: 3)")
	fmt.Println("  --debug, -d         Enable debug logging")
}
