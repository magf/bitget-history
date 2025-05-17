package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/magf/bitget-history/internal/cmdutils"
	"github.com/magf/bitget-history/internal/cmdutils/export"
	"github.com/magf/bitget-history/internal/db"
	"github.com/magf/bitget-history/internal/downloader"
	"github.com/magf/bitget-history/internal/proxymanager"
	"github.com/magf/bitget-history/internal/server/backend"
	"github.com/magf/bitget-history/internal/server/web"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
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
		Path         string `yaml:"path"`
		TempPath     string `yaml:"temp_path"`
		BackupSuffix string `yaml:"bak_suffix"`
	} `yaml:"database"`
	Datafiles struct {
		Path string `yaml:"path"`
	} `yaml:"datafiles"`
	Downloader struct {
		BaseURL   string `yaml:"base_url"`
		UserAgent string `yaml:"user_agent"`
	} `yaml:"downloader"`
}

func main() {
	// Парсим флаги
	helpFlag := flag.Bool("help", false, "Show help message")
	serverFlag := flag.Bool("server", false, "Run server")
	pairFlag := flag.String("pair", "BTCUSDT", "Trading pair (e.g., BTCUSDT)")
	typeFlag := flag.String("type", "", "Data type: trades or depth")
	marketFlag := flag.String("market", "all", "Market type: spot, futures or all")
	startFlag := flag.String("start", "", "Start date (YYYY-MM-DD, default: 1 year ago)")
	endFlag := flag.String("end", "", "End date (YYYY-MM-DD, default: today)")
	exportMT5 := flag.Bool("export-mt5", false, "Export data to MT5 CSV format")
	timeoutFlag := flag.Int("timeout", 3, "Proxy check timeout in seconds")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")
	skipExistsFlag := flag.Bool("skip-exists", false, "Skip downloading if file exists locally")
	repeatFlag := flag.Bool("repeat", false, "Repeat process until all files are downloaded (for --skip-exists only)")
	recheckExists := flag.Bool("recheck-exists", false, "Recheck existing non-zero archives for corruption")
	skipDownloadFlag := flag.Bool("skip-download", false, "Skip downloading and reimport existing local files")

	// Короткие флаги
	flag.BoolVar(helpFlag, "h", false, "Show help message (short)")
	flag.StringVar(pairFlag, "p", "BTCUSDT", "Trading pair (short)")
	flag.StringVar(typeFlag, "t", "", "Data type (short)")
	flag.StringVar(marketFlag, "m", "all", "Market type (short)")
	flag.StringVar(startFlag, "s", "", "Start date (short)")
	flag.StringVar(endFlag, "e", "", "End date (short)")
	flag.IntVar(timeoutFlag, "T", 3, "Proxy check timeout in seconds (short)")
	flag.BoolVar(debugFlag, "d", false, "Enable debug logging (short)")
	flag.BoolVar(skipExistsFlag, "X", false, "Skip downloading if file exists locally (short)")
	flag.BoolVar(repeatFlag, "r", false, "Repeat process until all files are downloaded (for --skip-exists only) (short)")
	flag.BoolVar(recheckExists, "R", false, "Recheck existing non-zero archives for corruption (short)")
	flag.BoolVar(skipDownloadFlag, "S", false, "Skip downloading and reimport existing local files (short)")

	flag.Parse()

	// Выводим справку, если указан --help или нет параметров
	if *helpFlag || len(os.Args) == 1 {
		cmdutils.PrintHelp()
		return
	}

	// Run server
	if *serverFlag {
		// Настраиваем единый сервер
		mux := http.NewServeMux()
		backend.StartServer(mux)
		web.StartServer(mux)
		log.Println("Server running on http://localhost:8080")
		if err := http.ListenAndServe(":8080", mux); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
		return
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

	// Формируем имя базы для проверенных URL-ов из cfg.Downloader.BaseURL
	// Пример: https://data.bitget.com → bitget_checked_urls.db
	baseURL := strings.TrimPrefix(cfg.Downloader.BaseURL, "https://")
	baseURL = strings.TrimPrefix(baseURL, "http://")
	baseURL = strings.Split(baseURL, "/")[0] // Берём домен
	baseURL = strings.ReplaceAll(baseURL, ".", "_")
	checkedUrlsDBName := fmt.Sprintf("%s_checked_urls.db", baseURL)
	checkedUrlsDBPath := filepath.Join(cfg.Database.Path, checkedUrlsDBName)
	if err := os.MkdirAll(filepath.Dir(checkedUrlsDBPath), 0755); err != nil {
		log.Fatalf("Failed to create directory for checked URLs database %s: %v", checkedUrlsDBPath, err)
	}
	// Открываем SQLite с WAL и shared cache для многопоточности
	checkedUrlsDB, err := sql.Open("sqlite3", checkedUrlsDBPath+"?_journal_mode=WAL&cache=shared")
	if err != nil {
		log.Fatalf("Failed to open checked URLs database %s: %v", checkedUrlsDBPath, err)
	}
	defer checkedUrlsDB.Close()

	// Создаём таблицу checked_urls, если не существует
	_, err = checkedUrlsDB.Exec(`
		CREATE TABLE IF NOT EXISTS checked_urls (
			url TEXT PRIMARY KEY,
			status_code INTEGER NOT NULL,
			content_length INTEGER NOT NULL,
			checked_at TIMESTAMP NOT NULL
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create checked_urls table: %v", err)
	}

	// Создаём ProxyManager
	timeout := time.Duration(*timeoutFlag) * time.Second
	pm, err := proxymanager.NewProxyManager(cfg.Proxy.RawFile, cfg.Proxy.WorkingFile, cfg.Proxy.Fallback, cfg.Proxy.Username, cfg.Proxy.Password, timeout)
	if err != nil {
		log.Fatalf("Failed to create proxy manager: %v", err)
	}

	// Создаём Downloader
	dl, err := downloader.NewDownloader(cfg.Downloader.BaseURL, cfg.Downloader.UserAgent, cfg.Datafiles.Path, pm, checkedUrlsDB)
	if err != nil {
		log.Fatalf("Failed to create downloader: %v", err)
	}

	// Проверяем существующие архивы, если указан флаг --recheck-exists
	if *recheckExists {
		log.Println("Rechecking existing archives...")
		brokenArchives, err := recheckExistingArchives(cfg.Datafiles.Path, *debugFlag)
		if err != nil {
			log.Fatalf("Failed to recheck archives: %v", err)
		}
		if len(brokenArchives) > 0 {
			log.Printf("Found %d broken archives. Starting redownload...", len(brokenArchives))
			redownloadBrokenArchives(brokenArchives, cfg, pm, dl)
		} else {
			log.Println("No broken archives found.")
		}
		return
	}

	// Проверяем обязательный флаг --type
	if *typeFlag == "" && !*exportMT5 {
		log.Fatal("Error: --type (trades or depth) or --export-mt5 is required")
	}

	// Проверяем логику флагов
	if *exportMT5 && *typeFlag != "" && *typeFlag != "depth" {
		log.Println("Warning: --export-mt5 is ignored for --type trades")
		*exportMT5 = false
	}

	if !*exportMT5 && *typeFlag != "trades" && *typeFlag != "depth" {
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

	// Проверяем путь к базе
	if cfg.Database.TempPath == "" || strings.Contains(cfg.Database.TempPath, "%s") {
		log.Fatalf("Error: invalid temp database path in config: %s", cfg.Database.TempPath)
	}
	log.Printf("Using temp database path from config: %s", cfg.Database.TempPath)

	if cfg.Database.Path == "" || strings.Contains(cfg.Database.Path, "%s") {
		log.Fatalf("Error: invalid root database path in config: %s", cfg.Database.Path)
	}
	log.Printf("Using root database path from config: %s", cfg.Database.Path)

	// Проверяем --repeat
	if *repeatFlag && !*skipExistsFlag {
		*repeatFlag = false
	}

	// Собираем все ZIP-файлы из директорий spot (1) и futures (2)
	marketCodes := []string{"1"} // spot
	if *marketFlag == "futures" {
		marketCodes = []string{"2"}
	} else if *marketFlag == "all" {
		marketCodes = []string{"1", "2"}
	}

	// Основной цикл
	if *typeFlag != "" {
		var proxies []string
		for {
			// Проверяем прокси, если не пропускаем загрузку
			if !*skipDownloadFlag {
				log.Println("Ensuring proxies...")
				if err := pm.EnsureProxies(context.Background()); err != nil {
					log.Printf("Warning: failed to ensure proxies: %v", err)
					if len(proxies) == 0 {
						log.Fatalf("No proxies available to continue")
					}
					log.Println("Continuing with last known proxies")
				} else {
					proxies, err = pm.GetProxies()
					if err != nil {
						log.Printf("Warning: failed to get proxies: %v", err)
						if len(proxies) == 0 {
							log.Fatalf("No proxies available to continue")
						}
						log.Println("Continuing with last known proxies")
					} else if len(proxies) == 0 {
						log.Fatalf("No working proxies found")
					} else {
						log.Printf("Found %d working proxies", len(proxies))
					}
				}
			}

			// Генерируем URL-ы
			log.Println("Generating URLs...")
			urls, err := cmdutils.GenerateURLs(dl, *marketFlag, *pairFlag, *typeFlag, startDate, endDate, *debugFlag, *skipExistsFlag, *skipDownloadFlag, cfg.Datafiles.Path)
			if err != nil {
				log.Fatalf("Failed to generate URLs: %v", err)
			}

			if !*skipDownloadFlag {
				// Запускаем загрузку
				fmt.Fprintln(os.Stdout)
				log.Println("Downloading files...")
				if err := dl.DownloadFiles(context.Background(), urls); err != nil {
					log.Printf("Warning: some files failed to download: %v", err)
				}
			}

			// Группируем ZIP-файлы по типу и рынку
			type ZipGroup struct {
				TempDbPath string
				dbPath     string
				files      []string
			}

			// Обрабатываем trades
			if *typeFlag == "trades" {
				log.Println("Processing Trades...")
				var zipGroups []ZipGroup
				spblFiles := make([]string, 0)
				umcblFiles := make([]string, 0)

				// Определяем директории в зависимости от marketFlag
				marketDirs := []string{}
				if *marketFlag == "spot" {
					marketDirs = append(marketDirs, "SPBL")
				} else if *marketFlag == "futures" {
					marketDirs = append(marketDirs, "UMCBL")
				} else if *marketFlag == "all" {
					marketDirs = append(marketDirs, "SPBL", "UMCBL")
				}

				// Собираем все ZIP-файлы из директорий
				for _, marketDir := range marketDirs {
					dir := filepath.Join(cfg.Datafiles.Path, "trades", marketDir, *pairFlag)
					if *debugFlag {
						log.Printf("Scanning directory: %s", dir)
					}
					err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
						if err != nil {
							log.Printf("Error accessing path %s: %v", path, err)
							return nil
						}
						if !info.IsDir() && strings.HasSuffix(info.Name(), ".zip") {
							// Фильтруем по датам
							dateStr := strings.Split(strings.TrimSuffix(info.Name(), ".zip"), "_")[0]
							if len(dateStr) != 8 {
								if *debugFlag {
									log.Printf("Skipping file %s: invalid date format", path)
								}
								return nil
							}
							fileDate, err := time.Parse("20060102", dateStr)
							if err != nil {
								if *debugFlag {
									log.Printf("Skipping file %s: cannot parse date %s", path, dateStr)
								}
								return nil
							}
							if !fileDate.Before(startDate) && !fileDate.After(endDate) {
								if marketDir == "SPBL" {
									spblFiles = append(spblFiles, path)
								} else if marketDir == "UMCBL" {
									umcblFiles = append(umcblFiles, path)
								}
								if *debugFlag {
									log.Printf("Added local file: %s", path)
								}
							}
						}
						return nil
					})
					if err != nil {
						log.Printf("Failed to walk directory %s: %v", dir, err)
					}
				}

				if (*marketFlag == "spot" || *marketFlag == "all") && len(spblFiles) > 0 {
					dbPath := filepath.Join(cfg.Database.Path, "trades", "SPBL", *pairFlag+".db")
					TempDbPath := filepath.Join(cfg.Database.TempPath, "trades", "SPBL", *pairFlag+".db")
					sort.Strings(spblFiles)
					log.Printf("Adding SPBL group: TempDbPath=%s, files=%v", TempDbPath, spblFiles)
					zipGroups = append(zipGroups, ZipGroup{dbPath: dbPath, TempDbPath: TempDbPath, files: spblFiles})
				}
				if (*marketFlag == "futures" || *marketFlag == "all") && len(umcblFiles) > 0 {
					dbPath := filepath.Join(cfg.Database.Path, "trades", "UMCBL", *pairFlag+".db")
					TempDbPath := filepath.Join(cfg.Database.TempPath, "trades", "UMCBL", *pairFlag+".db")
					sort.Strings(umcblFiles)
					log.Printf("Adding UMCBL group: TempDbPath=%s, files=%v", TempDbPath, umcblFiles)
					zipGroups = append(zipGroups, ZipGroup{dbPath: dbPath, TempDbPath: TempDbPath, files: umcblFiles})
				}
				if len(spblFiles) == 0 && len(umcblFiles) == 0 {
					log.Printf("No trades files found")
				}
				for _, group := range zipGroups {
					log.Printf("Processing database: %s with %d zip files", group.TempDbPath, len(group.files))
					if err := os.MkdirAll(filepath.Dir(group.TempDbPath), 0755); err != nil {
						log.Printf("Failed to create directory for %s: %v", group.TempDbPath, err)
						continue
					}
					// Для trades: копируем существующую БД из dbPath в TempDbPath, если она существует
					if _, err := os.Stat(group.dbPath); err == nil {
						if *debugFlag {
							log.Printf("Copying existing database from %s to %s", group.dbPath, group.TempDbPath)
						}
						srcFile, err := os.Open(group.dbPath)
						if err != nil {
							log.Printf("Failed to open source database %s: %v", group.dbPath, err)
							continue
						}
						defer srcFile.Close()
						dstFile, err := os.Create(group.TempDbPath)
						if err != nil {
							log.Printf("Failed to create temp database %s: %v", group.TempDbPath, err)
							continue
						}
						defer dstFile.Close()
						if _, err := io.Copy(dstFile, srcFile); err != nil {
							log.Printf("Failed to copy database from %s to %s: %v", group.dbPath, group.TempDbPath, err)
							continue
						}
					} else if *debugFlag {
						log.Printf("No existing database found at %s, creating new one at %s", group.dbPath, group.TempDbPath)
					}
					dbInstance, err := db.NewDB(group.TempDbPath, *typeFlag)
					if err != nil {
						log.Printf("Failed to create database %s: %v", group.TempDbPath, err)
						continue
					}
					if err := dbInstance.ProcessZipFiles(group.files, *debugFlag); err != nil {
						log.Printf("Failed to process zip files for %s: %v", group.TempDbPath, err)
					}
					if err := dbInstance.Close(); err != nil {
						log.Printf("Failed to close database %s: %v", group.TempDbPath, err)
					}
					if err := cmdutils.MoveTempDatabase(group.TempDbPath, group.dbPath, cfg.Database.BackupSuffix, *debugFlag); err != nil {
						log.Fatalf("Error: %v\n", err)
					}
				}
			}

			// Обрабатываем depth
			if *typeFlag == "depth" {
				log.Println("Processing Depth...")
				dbPath := filepath.Join(cfg.Database.Path, "depth", *pairFlag+".db")
				TempDbPath := filepath.Join(cfg.Database.TempPath, "depth", *pairFlag+".db")
				var depthFiles []string

				for _, marketCode := range marketCodes {
					dir := filepath.Join(cfg.Datafiles.Path, "depth", *pairFlag, marketCode)
					if *debugFlag {
						log.Printf("Scanning directory: %s", dir)
					}
					err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
						if err != nil {
							log.Printf("Error accessing path %s: %v", path, err)
							return nil
						}
						if !info.IsDir() && strings.HasSuffix(info.Name(), ".zip") {
							// Фильтруем по датам
							dateStr := strings.Split(strings.TrimSuffix(info.Name(), ".zip"), "_")[0]
							if len(dateStr) != 8 {
								if *debugFlag {
									log.Printf("Skipping file %s: invalid date format", path)
								}
								return nil
							}
							fileDate, err := time.Parse("20060102", dateStr)
							if err != nil {
								if *debugFlag {
									log.Printf("Skipping file %s: cannot parse date %s", path, dateStr)
								}
								return nil
							}
							if !fileDate.Before(startDate) && !fileDate.After(endDate) {
								depthFiles = append(depthFiles, path)
								if *debugFlag {
									log.Printf("Added local file: %s", path)
								}
							}
						}
						return nil
					})
					if err != nil {
						log.Printf("Failed to walk directory %s: %v", dir, err)
					}
				}

				if len(depthFiles) > 0 {
					// Сортируем файлы в алфавитном порядке
					sort.Strings(depthFiles)
					log.Printf("Processing depth database: %s with %d zip files", TempDbPath, len(depthFiles))

					// Создаём директорию для базы
					if err := os.MkdirAll(filepath.Dir(TempDbPath), 0755); err != nil {
						log.Printf("Failed to create directory for %s: %v", TempDbPath, err)
					} else {
						// Обрабатываем базу
						dbInstance, err := db.NewDB(TempDbPath, *typeFlag)
						if err != nil {
							log.Printf("Failed to create database %s: %v", TempDbPath, err)
						} else {
							if err := dbInstance.ProcessZipFiles(depthFiles, *debugFlag); err != nil {
								log.Printf("Failed to process zip files for %s: %v", TempDbPath, err)
							}
							if err := dbInstance.Close(); err != nil {
								log.Printf("Failed to close database %s: %v", TempDbPath, err)
							}
						}
					}
				} else {
					log.Printf("No depth files found for %s", TempDbPath)
				}
				if err := cmdutils.MoveTempDatabase(TempDbPath, dbPath, cfg.Database.BackupSuffix, *debugFlag); err != nil {
					log.Fatalf("Error: %v\n", err)
				}
			}
			log.Printf("Repeat cycle: %d URLs remaining, continuing...", len(urls))

			// Проверяем, нужно ли повторять
			if !*repeatFlag || len(urls) == 0 {
				if *repeatFlag && len(urls) == 0 {
					log.Println("Repeat cycle completed: no URLs remaining")
				}
				break
			}
		}
	}
	// Экспорт в MT5 CSV (если указан --export-mt5)
	if *exportMT5 {
		for _, marketCode := range marketCodes {
			dbPath := filepath.Join(cfg.Database.Path, "depth", *pairFlag+".db")
			outputFile, err := export.ExportToMT5CSV(dbPath, *pairFlag, marketCode, "m1", startDate, endDate)
			if err != nil {
				log.Printf("Failed to export to MT5 CSV: %v", err)
			} else {
				fmt.Println(outputFile) // Выводим имя файла в stdout
			}
		}
	}

	log.Println("Processing completed successfully")
}

// recheckExistingArchives проверяет все ненулевые ZIP-архивы в директории и возвращает список битых
func recheckExistingArchives(rootDir string, debug bool) ([]string, error) {
	var brokenArchives []string
	log.Println("Rechecking existing archives...")
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %s: %v", path, err)
			return nil // Пропускаем проблемные пути
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".zip") {
			if info.Size() == 0 {
				if debug {
					log.Printf("Skipping zero-sized archive: %s", path)
				}
				return nil
			}
			if debug {
				log.Printf("Checking archive: %s", path)
			}
			// Проверяем, что файл является Zip
			if err := downloader.CheckZipFile(path); err != nil {
				if debug {
					log.Printf("Archive %s is broken", path)
				} else {
					fmt.Fprintf(os.Stdout, "\rArchive %s is broken", path)
				}
				brokenArchives = append(brokenArchives, path)
			} else {
				if debug {
					log.Printf("Archive %s is valid", path)
				} else {
					fmt.Fprintf(os.Stdout, "\rArchive %s is valid", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", rootDir, err)
	}
	log.Println("Recheck done.")
	return brokenArchives, nil
}

// redownloadBrokenArchives перезагружает битые архивы через валидные прокси
func redownloadBrokenArchives(brokenArchives []string, cfg Config, pm *proxymanager.ProxyManager, dl *downloader.Downloader) {
	// Обновляем прокси
	log.Println("Ensuring proxies for redownload...")
	var proxies []string
	if err := pm.EnsureProxies(context.Background()); err != nil {
		log.Printf("Warning: failed to ensure proxies: %v", err)
		proxies, err = pm.GetProxies()
		if err != nil || len(proxies) == 0 {
			log.Fatalf("No proxies available to continue")
		}
		log.Println("Continuing with last known proxies")
	} else {
		var err error
		proxies, err = pm.GetProxies()
		if err != nil || len(proxies) == 0 {
			log.Fatalf("No working proxies found")
		}
		log.Printf("Found %d working proxies", len(proxies))
	}

	// Формируем список файлов для загрузки
	urls := make([]downloader.FileInfo, 0, len(brokenArchives))
	for _, archive := range brokenArchives {
		// Получаем относительный путь
		relPath, err := filepath.Rel(cfg.Datafiles.Path, archive)
		if err != nil {
			log.Printf("Failed to get relative path for %s: %v", archive, err)
			continue
		}
		// Формируем URL
		url := fmt.Sprintf("%s/%s", strings.TrimSuffix(cfg.Downloader.BaseURL, "/"), relPath)
		urls = append(urls, downloader.FileInfo{
			URL:           url,
			ContentLength: 0, // Не знаем размер заранее
		})
	}

	if len(urls) == 0 {
		log.Println("No valid URLs generated for broken archives")
		return
	}

	// Запускаем загрузку
	fmt.Fprintln(os.Stdout)
	log.Printf("Redownloading %d broken archives...", len(urls))
	if err := dl.DownloadFiles(context.Background(), urls); err != nil {
		log.Printf("Warning: some files failed to redownload: %v", err)
	} else {
		log.Println("Redownload completed successfully")
	}
}
