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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/magf/bitget-history/internal/db"
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
	marketFlag := flag.String("market", "all", "Market type: spot, futures or all")
	startFlag := flag.String("start", "", "Start date (YYYY-MM-DD, default: 1 year ago)")
	endFlag := flag.String("end", "", "End date (YYYY-MM-DD, default: today)")
	timeoutFlag := flag.Int("timeout", 3, "Proxy check timeout in seconds")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")

	// Короткие флаги
	flag.BoolVar(helpFlag, "h", false, "Show help message (short)")
	flag.StringVar(pairFlag, "p", "BTCUSDT", "Trading pair (short)")
	flag.StringVar(typeFlag, "t", "", "Data type (short)")
	flag.StringVar(marketFlag, "m", "all", "Market type (short)")
	flag.StringVar(startFlag, "s", "", "Start date (short)")
	flag.StringVar(endFlag, "e", "", "End date (short)")
	flag.IntVar(timeoutFlag, "T", 3, "Proxy check timeout in seconds (short)")
	flag.BoolVar(debugFlag, "d", false, "Enable debug logging (short)")

	flag.Parse()

	// Выводим справку, если указан --help или нет параметров
	if *helpFlag || len(os.Args) == 1 {
		printHelp()
		return
	}

	// Проверяем обязательный флаг --type
	if *typeFlag == "" {
		log.Fatal("Error: --type (-t) is required (trades or depth)")
	}
	if *typeFlag != "trades" && *typeFlag != "depth" {
		log.Fatalf("Error: invalid --type value: %s (must be trades or depth)", *typeFlag)
	}

	// Проверяем market
	if *marketFlag != "spot" && *marketFlag != "futures" && *marketFlag != "all" {
		log.Fatalf("Error: invalid --market value: %s (must be spot, futures or all)", *marketFlag)
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

	// Проверяем путь к базе
	if cfg.Database.Path == "" || strings.Contains(cfg.Database.Path, "%s") {
		log.Fatalf("Error: invalid database path in config: %s", cfg.Database.Path)
	}
	log.Printf("Using database root path from config: %s", cfg.Database.Path)

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

	// Группируем ZIP-файлы по типу и рынку
	type ZipGroup struct {
		dbPath string
		files  []string
	}
	var zipGroups []ZipGroup

	// Нормализуем BaseURL для TrimPrefix
	baseURLPrefix := strings.TrimSuffix(cfg.Downloader.BaseURL, "/") + "/"
	log.Printf("Using baseURLPrefix for trimming: %s", baseURLPrefix)

	if *typeFlag == "depth" {
		// Для depth одна база: depth/<pair>.db
		dbPath := filepath.Join(cfg.Database.Path, "depth", *pairFlag+".db")
		var depthFiles []string
		for _, fileInfo := range urls {
			relativePath := strings.TrimPrefix(fileInfo.URL, baseURLPrefix)
			zipPath := filepath.Join(outputDir, relativePath)
			if *debugFlag {
				log.Printf("Processing URL: %s, relativePath: %s, zipPath: %s", fileInfo.URL, relativePath, zipPath)
			}
			if strings.Contains(strings.ToLower(relativePath), "depth/") {
				depthFiles = append(depthFiles, zipPath)
			}
		}
		if len(depthFiles) > 0 {
			sort.Strings(depthFiles)
			log.Printf("Adding depth group: dbPath=%s, files=%v", dbPath, depthFiles)
			zipGroups = append(zipGroups, ZipGroup{dbPath: dbPath, files: depthFiles})
		} else {
			log.Printf("No depth files found for %s", dbPath)
		}
	} else if *typeFlag == "trades" {
		// Для trades две базы: trades/SPBL/<pair>.db и trades/UMCBL/<pair>.db
		spblFiles := make([]string, 0)
		umcblFiles := make([]string, 0)
		for _, fileInfo := range urls {
			relativePath := strings.TrimPrefix(fileInfo.URL, baseURLPrefix)
			zipPath := filepath.Join(outputDir, relativePath)
			if *debugFlag {
				log.Printf("Processing URL: %s, relativePath: %s, zipPath: %s", fileInfo.URL, relativePath, zipPath)
			}
			if strings.Contains(strings.ToLower(relativePath), "trades/") {
				if strings.Contains(relativePath, "/SPBL/") {
					spblFiles = append(spblFiles, zipPath)
				} else if strings.Contains(relativePath, "/UMCBL/") {
					umcblFiles = append(umcblFiles, zipPath)
				}
			}
		}
		// Добавляем группы, если файлы есть
		if (*marketFlag == "spot" || *marketFlag == "all") && len(spblFiles) > 0 {
			dbPath := filepath.Join(cfg.Database.Path, "trades", "SPBL", *pairFlag+".db")
			sort.Strings(spblFiles)
			log.Printf("Adding SPBL group: dbPath=%s, files=%v", dbPath, spblFiles)
			zipGroups = append(zipGroups, ZipGroup{dbPath: dbPath, files: spblFiles})
		}
		if (*marketFlag == "futures" || *marketFlag == "all") && len(umcblFiles) > 0 {
			dbPath := filepath.Join(cfg.Database.Path, "trades", "UMCBL", *pairFlag+".db")
			sort.Strings(umcblFiles)
			log.Printf("Adding UMCBL group: dbPath=%s, files=%v", dbPath, umcblFiles)
			zipGroups = append(zipGroups, ZipGroup{dbPath: dbPath, files: umcblFiles})
		}
		if len(spblFiles) == 0 && len(umcblFiles) == 0 {
			log.Printf("No trades files found")
		}
	}

	// Обрабатываем каждую группу
	for _, group := range zipGroups {
		log.Printf("Processing database: %s with %d zip files", group.dbPath, len(group.files))
		// Создаём каталог для базы
		if err := os.MkdirAll(filepath.Dir(group.dbPath), 0755); err != nil {
			log.Printf("Failed to create directory for %s: %v", group.dbPath, err)
			continue
		}
		// Создаём DB
		dbInstance, err := db.NewDB(group.dbPath, *typeFlag)
		if err != nil {
			log.Printf("Failed to create database %s: %v", group.dbPath, err)
			continue
		}
		// Обрабатываем файлы
		if err := dbInstance.ProcessZipFiles(group.files); err != nil {
			log.Printf("Failed to process zip files for %s: %v", group.dbPath, err)
		}
		// Закрываем базу
		if err := dbInstance.Close(); err != nil {
			log.Printf("Failed to close database %s: %v", group.dbPath, err)
		}
	}

	log.Println("Processing completed successfully")

	// Явный чекпоинт перед завершением
	for _, group := range zipGroups {
		dbInstance, err := db.NewDB(group.dbPath, *typeFlag)
		if err != nil {
			log.Printf("Failed to reopen database %s for checkpoint: %v", group.dbPath, err)
			continue
		}
		err = dbInstance.Close()
		if err != nil {
			log.Printf("Failed to perform final WAL checkpoint for %s: %v", group.dbPath, err)
		} else {
			log.Printf("Final WAL checkpoint successful for %s", group.dbPath)
		}
	}
}

// generateURLs генерирует список URL-ов на основе параметров.
func generateURLs(baseURL, market, pair, dataType string, startDate, endDate time.Time, debug bool, proxies []string, userAgent string) ([]downloader.FileInfo, error) {
	var urls []downloader.FileInfo
	days := int(endDate.Sub(startDate).Hours()/24) + 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	if dataType == "trades" {
		marketCodes := []string{"SPBL"} // spot по умолчанию
		if market == "futures" {
			marketCodes = []string{"UMCBL"}
		} else if market == "all" {
			marketCodes = []string{"SPBL", "UMCBL"} // Поддержка all
		}
		for _, marketCode := range marketCodes {
			for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
				dateStr := d.Format("20060102")
				// Проверяем файлы пачками по 10
				for startNum := 1; startNum <= 999; startNum += 10 {
					endNum := startNum + 9
					if endNum > 999 {
						endNum = 999
					}
					batchURLs := make([]string, 0, endNum-startNum+1)
					for num := startNum; num <= endNum; num++ {
						path := fmt.Sprintf("trades/%s/%s/%s_%03d.zip", marketCode, pair, dateStr, num)
						url := fmt.Sprintf("%s/%s", baseURL, path)
						batchURLs = append(batchURLs, url)
					}

					// Параллельная проверка пачки
					stopBatch := false
					for _, url := range batchURLs {
						wg.Add(1)
						go func(url string) {
							defer wg.Done()
							exists, contentLength, err := checkFileExists(url, proxies, userAgent, debug)
							if err != nil {
								if debug {
									log.Printf("Error checking %s: %v", url, err)
								}
								return
							}
							if !exists {
								if debug {
									log.Printf("Stopping at %s: file does not exist", url)
								}
								mu.Lock()
								stopBatch = true
								mu.Unlock()
								return
							}
							mu.Lock()
							urls = append(urls, downloader.FileInfo{URL: url, ContentLength: contentLength})
							if debug {
								log.Printf("Generated URL: %s (Content-Length: %d)", url, contentLength)
							}
							mu.Unlock()
						}(url)
					}
					wg.Wait()
					if stopBatch {
						break // Прерываем цикл для этой даты
					}
				}
			}
		}
	} else { // depth
		// Выбираем marketCodes в зависимости от --market
		marketCodes := []string{"1"} // spot по умолчанию
		if market == "futures" {
			marketCodes = []string{"2"}
		} else if market == "all" {
			marketCodes = []string{"1", "2"}
		}
		for _, marketCode := range marketCodes {
			for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
				path := fmt.Sprintf("depth/%s/%s/%s.zip", pair, marketCode, d.Format("20060102"))
				url := fmt.Sprintf("%s/%s", baseURL, path)

				wg.Add(1)
				go func(url string) {
					defer wg.Done()
					exists, contentLength, err := checkFileExists(url, proxies, userAgent, debug)
					if err != nil {
						if debug {
							log.Printf("Error checking %s: %v", url, err)
						}
						return
					}
					if !exists {
						if debug {
							log.Printf("Skipping %s: file does not exist", url)
						}
						return
					}
					mu.Lock()
					urls = append(urls, downloader.FileInfo{URL: url, ContentLength: contentLength})
					if debug {
						log.Printf("Generated URL: %s (Content-Length: %d)", url, contentLength)
					}
					mu.Unlock()
				}(url)
			}
		}
	}

	wg.Wait()

	// Вычисляем коэффициент ротации прокси
	proxyCount, err := readProxyCount()
	if err != nil {
		return nil, fmt.Errorf("failed to read proxy count: %w", err)
	}
	rotationFactor := int(math.Ceil(float64(days) / float64(proxyCount)))
	if debug {
		log.Printf("Days: %d, Proxies: %d, Rotation factor: %d", days, proxyCount, rotationFactor)
		log.Printf("Generated %d URLs: %v", len(urls), urls)
	}

	return urls, nil
}

// checkFileExists проверяет существование файла через HEAD-запрос и возвращает Content-Length.
func checkFileExists(urlStr string, proxies []string, userAgent string, debug bool) (bool, int64, error) {
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
		defer resp.Body.Close()

		if debug {
			log.Printf("Checked %s with proxy %s: status %d", urlStr, proxyURLStr, resp.StatusCode)
		}
		// Явно считаем файл существующим только при 200-399, иначе прерываем при 400+
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			contentLengthStr := resp.Header.Get("Content-Length")
			contentLength, _ := strconv.ParseInt(contentLengthStr, 10, 64) // Игнорируем ошибку, если заголовок отсутствует
			return true, contentLength, nil
		} else if resp.StatusCode >= 400 {
			return false, 0, nil
		}
	}
	if debug {
		log.Printf("File %s skipped after %d attempts due to error: %v", urlStr, maxAttempts, lastErr)
	}
	return false, 0, fmt.Errorf("failed to check %s after %d attempts: %v", urlStr, maxAttempts, lastErr)
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
	fmt.Println("  --market, -m string Market type: spot, futures or all (default: all)")
	fmt.Println("  --start, -s string  Start date (YYYY-MM-DD, default: 1 year ago)")
	fmt.Println("  --end, -e string    End date (YYYY-MM-DD, default: today)")
	fmt.Println("  --timeout, -T int   Proxy check timeout in seconds (default: 3)")
	fmt.Println("  --debug, -d         Enable debug logging")
}
