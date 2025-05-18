package cmdutils

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/magf/bitget-history/internal/downloader"
	"gopkg.in/yaml.v3"
)

// GenerateURLs генерирует список URL-ов на основе параметров.
func GenerateURLs(dl *downloader.Downloader, market, pair, dataType string, startDate, endDate time.Time, debug, skipIfExists, skipDownload bool, outputDir string) ([]downloader.FileInfo, error) {
	var urls []downloader.FileInfo
	var mu sync.Mutex
	var wg sync.WaitGroup

	if dataType == "trades" {
		marketCodes := []string{"SPBL"} // spot по умолчанию
		if market == "futures" {
			marketCodes = []string{"UMCBL"}
		} else if market == "all" {
			marketCodes = []string{"SPBL", "UMCBL"}
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
					batchPaths := make([]string, 0, endNum-startNum+1)
					for num := startNum; num <= endNum; num++ {
						path := fmt.Sprintf("trades/%s/%s/%s_%03d.zip", marketCode, pair, dateStr, num)
						url := fmt.Sprintf("%s/%s", strings.TrimSuffix(dl.BaseURL, "/"), path)
						batchURLs = append(batchURLs, url)
						batchPaths = append(batchPaths, path)
					}

					// Параллельная проверка пачки
					stopBatch := false
					for i, url := range batchURLs {
						wg.Add(1)
						go func(url, path string) {
							defer wg.Done()

							// Пропускаем скачивание, если установлен --skip-download
							if skipDownload {
								mu.Lock()
								urls = append(urls, downloader.FileInfo{URL: url, ContentLength: 0})
								mu.Unlock()
								return
							}

							// Проверяем, существует ли файл локально, если установлен --skip-exists
							if skipIfExists {
								localPath := filepath.Join(outputDir, path)
								if _, err := os.Stat(localPath); err == nil {
									if debug {
										log.Printf("Skipping %s: file already exists locally", url)
									}
									mu.Lock()
									urls = append(urls, downloader.FileInfo{URL: url, ContentLength: 0})
									mu.Unlock()
									return
								}
							}

							// Проверяем доступность URL
							statusCode, contentLength, err := dl.CheckFileOnline(url, debug)
							if err != nil {
								if debug {
									log.Printf("Error checking %s: %v", url, err)
								}
								return
							}
							if statusCode != 200 {
								if debug {
									log.Printf("Skipping %s: status code %d", url, statusCode)
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
							} else {
								fmt.Fprintf(os.Stdout, "\r  Generated URL: %-90s (Content-Length: %d)                    \r", url, contentLength)
							}
							mu.Unlock()
						}(url, batchPaths[i])
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
				url := fmt.Sprintf("%s/%s", strings.TrimSuffix(dl.BaseURL, "/"), path)

				wg.Add(1)
				go func(url, path string) {
					defer wg.Done()

					// Проверяем, существует ли файл локально, если установлен --skip-exists
					if skipIfExists {
						localPath := filepath.Join(outputDir, path)
						if _, err := os.Stat(localPath); err == nil {
							if debug {
								log.Printf("Skipping %s: file already exists locally", url)
							}
							mu.Lock()
							urls = append(urls, downloader.FileInfo{URL: url, ContentLength: 0})
							mu.Unlock()
							return
						}
					}

					// Пропускаем проверку, если установлен --skip-download
					if skipDownload {
						mu.Lock()
						urls = append(urls, downloader.FileInfo{URL: url, ContentLength: 0})
						mu.Unlock()
						return
					}

					// Проверяем доступность URL
					statusCode, contentLength, err := dl.CheckFileOnline(url, debug)
					if err != nil {
						if debug {
							log.Printf("Error checking %s: %v", url, err)
						}
						return
					}
					if statusCode != 200 {
						if statusCode == 403 || statusCode == 404 {
							// Создаём пустой файл для depth
							localPath := filepath.Join(outputDir, path)
							if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
								if debug {
									log.Printf("Failed to create directory for %s: %v", localPath, err)
								}
								return
							}
							if err := os.WriteFile(localPath, []byte{}, 0644); err != nil {
								if debug {
									log.Printf("Failed to create empty file %s: %v", localPath, err)
								}
								return
							}
							if debug {
								log.Printf("Created empty file %s for status %d", localPath, statusCode)
							}
						} else if debug {
							log.Printf("Skipping %s: status code %d", url, statusCode)
						}
						return
					}
					mu.Lock()
					urls = append(urls, downloader.FileInfo{URL: url, ContentLength: contentLength})
					if debug {
						log.Printf("Generated URL: %s (Content-Length: %d)", url, contentLength)
					} else {
						fmt.Fprintf(os.Stdout, "\r  Generated URL: %-90s (Content-Length: %d)                    \r", url, contentLength)
					}
					mu.Unlock()
				}(url, path)
			}
		}
	}

	wg.Wait()

	return urls, nil
}

// ReadProxyCount читает количество прокси из working_file.
func ReadProxyCount() (int, error) {
	cfg := struct {
		Proxy struct {
			WorkingFile string `yaml:"working_file"`
		} `yaml:"proxy"`
	}{}
	data, err := os.ReadFile("config/config.yaml")
	if err != nil {
		return 0, fmt.Errorf("failed to read config.yaml: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return 0, fmt.Errorf("failed to parse config.yaml: %w", err)
	}

	file, err := os.Open(cfg.Proxy.WorkingFile)
	if err != nil {
		return 0, fmt.Errorf("failed to open working proxies file %s: %w", cfg.Proxy.WorkingFile, err)
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("failed to read working proxies file: %w", err)
	}
	return count, nil
}

// MoveTempDatabase переименовывает существующую базу в файл с указанным расширением и перемещает временную базу на её место.
func MoveTempDatabase(TempDbPath, dbPath, BackupSuffix string, debug bool) error {
	backupPath := dbPath + BackupSuffix
	if _, err := os.Stat(dbPath); err == nil {
		if err := os.Rename(dbPath, backupPath); err != nil {
			return fmt.Errorf("failed to backup database %s to %s: %w", dbPath, backupPath, err)
		}
		if debug {
			log.Printf("Backed up database to %s", backupPath)
		}
	}
	srcFile, err := os.Open(TempDbPath)
	if err != nil {
		if _, err := os.Stat(backupPath); err == nil {
			os.Rename(backupPath, dbPath)
		}
		return fmt.Errorf("failed to open temporary database %s: %w", TempDbPath, err)
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dbPath)
	if err != nil {
		if _, err := os.Stat(backupPath); err == nil {
			os.Rename(backupPath, dbPath)
		}
		return fmt.Errorf("failed to create database %s: %w", dbPath, err)
	}
	defer dstFile.Close()
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		if _, err := os.Stat(backupPath); err == nil {
			os.Rename(backupPath, dbPath)
		}
		return fmt.Errorf("failed to copy temporary database %s to %s: %w", TempDbPath, dbPath, err)
	}
	if err := dstFile.Sync(); err != nil {
		if _, err := os.Stat(backupPath); err == nil {
			os.Rename(backupPath, dbPath)
		}
		return fmt.Errorf("failed to sync database %s: %w", dbPath, err)
	}
	if debug {
		log.Printf("Copied temporary database to %s", dbPath)
	}
	if err := os.Remove(TempDbPath); err != nil {
		log.Printf("Warning: failed to remove temporary database %s: %v", TempDbPath, err)
	}
	return nil
}

// PrintHelp выводит справку по флагам.
func PrintHelp() {
	fmt.Println("Usage: bitget-history [options]")
	fmt.Println("Options:")
	fmt.Println("  -h, --help            Show this help message")
	fmt.Println("  -p, --pair string     Trading pair (e.g., BTCUSDT) (default: BTCUSDT)")
	fmt.Println("  -t, --type string     Data type: trades or depth (required)")
	fmt.Println("  -m, --market string   Market type: spot, futures, or all (default: all)")
	fmt.Println("  -s, --start string    Start date (YYYY-MM-DD) (default: 1 year ago)")
	fmt.Println("  -e, --end string      End date (YYYY-MM-DD) (default: today)")
	fmt.Println("  -T, --timeout int     Proxy check timeout in seconds (default: 3)")
	fmt.Println("  -d, --debug           Enable debug logging")
	fmt.Println("  -X, --skip-exists 	 Skip downloading if file exists locally")
	fmt.Println("  -S, --skip-download   Skip downloading and reimport existing local files")
	fmt.Println("  -r, --repeat          Repeat process until all files are downloaded (for -S, --skip-exists only)")
	fmt.Println("  -R, --recheck-exists  Recheck existing non-zero archives for corruptio")
}
