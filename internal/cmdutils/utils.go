package cmdutils

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/magf/bitget-history/internal/downloader"
	"golang.org/x/net/proxy"
)

// GenerateURLs генерирует список URL-ов на основе параметров.
func GenerateURLs(baseURL, market, pair, dataType string, startDate, endDate time.Time, debug, skipIfExists bool, proxies []string, userAgent, outputDir string) ([]downloader.FileInfo, error) {
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
							exists, contentLength, err := CheckFileExists(url, proxies, userAgent, debug)
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
				go func(url, path string) {
					defer wg.Done()

					// Проверяем, существует ли файл локально, если установлен --skip-if-exists
					if skipIfExists {
						localPath := filepath.Join(outputDir, path)
						if _, err := os.Stat(localPath); err == nil {
							if debug {
								log.Printf("Skipping %s: file already exists locally", url)
							}
							return
						}
					}

					exists, contentLength, err := CheckFileExists(url, proxies, userAgent, debug)
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
				}(url, path)
			}
		}
	}

	wg.Wait()

	// Вычисляем коэффициент ротации прокси
	proxyCount, err := ReadProxyCount()
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

// CheckFileExists проверяет существование файла через HEAD-запрос и возвращает Content-Length.
func CheckFileExists(urlStr string, proxies []string, userAgent string, debug bool) (bool, int64, error) {
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

// ReadProxyCount читает количество прокси из файла.
func ReadProxyCount() (int, error) {
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

// PrintHelp выводит справку.
func PrintHelp() {
	fmt.Println("Bitget History Downloader")
	fmt.Println("Usage: bitget-history [options]")
	fmt.Println("Options:")
	fmt.Println("  --help, -h          		Show this help message")
	fmt.Println("  --pair, -p string   		Trading pair (e.g., BTCUSDT) (default: BTCUSDT)")
	fmt.Println("  --type, -t string   		Data type: trades or depth (required)")
	fmt.Println("  --market, -m string 		Market type: spot, futures or all (default: all)")
	fmt.Println("  --start, -s string  		Start date (YYYY-MM-DD, default: 1 year ago)")
	fmt.Println("  --end, -e string    		End date (YYYY-MM-DD, default: today)")
	fmt.Println("  --timeout, -T int   		Proxy check timeout in seconds (default: 3)")
	fmt.Println("  --debug, -d         		Enable debug logging")
	fmt.Println("  --skip-if-exists, -S		Skip downloading if file exists locally (for depth only)")
	fmt.Println("  --repeat, -r        		Repeat process until all files are downloaded (for depth with --skip-if-exists only)")
}
