package downloader

import (
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/magf/bitget-history/internal/proxymanager"
	"golang.org/x/net/proxy"

	_ "github.com/bdandy/go-socks4" // Поддержка SOCKS4
)

// Downloader управляет загрузкой файлов.
type Downloader struct {
	BaseURL       string
	userAgent     string
	outputDir     string
	proxyMgr      *proxymanager.ProxyManager
	maxRetries    int
	checkedUrlsDB *sql.DB
}

// FileInfo хранит информацию о файле.
type FileInfo struct {
	URL           string
	ContentLength int64
}

// NewDownloader создаёт новый загрузчик.
func NewDownloader(baseURL, userAgent, outputDir string, proxyMgr *proxymanager.ProxyManager, checkedUrlsDB *sql.DB) (*Downloader, error) {
	return &Downloader{
		BaseURL:       baseURL,
		userAgent:     userAgent,
		outputDir:     outputDir,
		proxyMgr:      proxyMgr,
		maxRetries:    5,
		checkedUrlsDB: checkedUrlsDB,
	}, nil
}

// CheckFileOnline проверяет доступность файла по URL и возвращает код состояния и размер.
func (d *Downloader) CheckFileOnline(urlStr string, debug bool) (statusCode int, contentLength int64, err error) {
	// Проверяем, есть ли URL в базе
	var checkedAt time.Time
	err = d.checkedUrlsDB.QueryRow(`
		SELECT status_code, content_length, checked_at
		FROM checked_urls
		WHERE url = ?
	`, urlStr).Scan(&statusCode, &contentLength, &checkedAt)
	if err == nil {
		if debug {
			log.Printf("Found cached URL %s: status=%d, size=%d, checked_at=%s", urlStr, statusCode, contentLength, checkedAt)
		}
		return statusCode, contentLength, nil
	}
	if err != sql.ErrNoRows {
		log.Printf("Failed to query checked_urls for %s: %v", urlStr, err)
	}

	// Если в базе нет, делаем HEAD-запрос
	proxies, err := d.proxyMgr.GetProxies()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get proxies: %w", err)
	}
	if len(proxies) == 0 {
		return 0, 0, fmt.Errorf("no proxies available")
	}

	proxyURL, err := url.Parse(proxies[rand.Intn(len(proxies))])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid proxy URL: %w", err)
	}

	dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create proxy %s: %w", proxyURL.String(), err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: dialer.Dial,
		},
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("HEAD", urlStr, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create request for %s: %w", urlStr, err)
	}
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to check %s: %w", urlStr, err)
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	contentLength = resp.ContentLength
	if debug {
		log.Printf("Checked URL %s: status=%d, size=%d", urlStr, statusCode, contentLength)
	}

	// Сохраняем результат в базу
	_, err = d.checkedUrlsDB.Exec(`
		INSERT OR REPLACE INTO checked_urls (url, status_code, content_length, checked_at)
		VALUES (?, ?, ?, ?)
	`, urlStr, statusCode, contentLength, time.Now())
	if err != nil {
		log.Printf("Failed to save URL %s to checked_urls: %v", urlStr, err)
	}

	return statusCode, contentLength, nil
}

// DownloadFiles загружает файлы по списку URL-ов.
func (d *Downloader) DownloadFiles(ctx context.Context, files []FileInfo) error {
	log.Printf("Starting download of %d files", len(files))
	var wg sync.WaitGroup
	errChan := make(chan error, len(files))
	failedURLs := make([]string, 0)
	var mu sync.Mutex
	badProxies := make(map[string]struct{}) // Кэш нерабочих прокси

	for i, file := range files {
		wg.Add(1)
		go func(i int, file FileInfo) {
			defer wg.Done()
			// Проверяем, существует ли файл и совпадает ли размер
			relativePath := strings.TrimPrefix(file.URL, d.BaseURL+"/")
			outputPath := filepath.Join(d.outputDir, relativePath)
			if file.ContentLength > 0 {
				if stat, err := os.Stat(outputPath); err == nil && stat.Size() == file.ContentLength {
					log.Printf("Skipping %s: file exists with correct size %d", file.URL, file.ContentLength)
					return
				}
			}

			log.Printf("Downloading file %d: %s", i+1, file.URL)
			for attempt := 1; attempt <= d.maxRetries; attempt++ {
				proxies, err := d.proxyMgr.GetProxies()
				if err != nil {
					log.Printf("Failed to get proxies: %v", err)
					errChan <- err
					return
				}
				if len(proxies) == 0 {
					log.Printf("No proxies available")
					errChan <- fmt.Errorf("no proxies available")
					return
				}

				// Фильтруем нерабочие прокси
				var availableProxies []string
				for _, p := range proxies {
					if _, bad := badProxies[p]; !bad {
						availableProxies = append(availableProxies, p)
					}
				}
				if len(availableProxies) == 0 {
					log.Printf("All proxies marked as bad for %s", file.URL)
					mu.Lock()
					failedURLs = append(failedURLs, file.URL)
					mu.Unlock()
					errChan <- fmt.Errorf("no good proxies left for %s", file.URL)
					return
				}

				proxyIndex := rand.Intn(len(availableProxies))
				proxyURL := availableProxies[proxyIndex]
				log.Printf("Attempt %d/%d for %s using proxy %s", attempt, d.maxRetries, file.URL, proxyURL)

				err = d.downloadWithProxy(ctx, file.URL, proxyURL)
				if err == nil {
					return
				}
				log.Printf("Failed attempt %d for %s with proxy %s: %v", attempt, file.URL, proxyURL, err)
				// Помечаем прокси как нерабочий при определённых ошибках
				if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "timeout") {
					badProxies[proxyURL] = struct{}{}
					log.Printf("Marked proxy %s as bad", proxyURL)
				}
				time.Sleep(time.Second * time.Duration(attempt))
			}
			mu.Lock()
			failedURLs = append(failedURLs, file.URL)
			mu.Unlock()
			errChan <- fmt.Errorf("failed to download %s after %d attempts", file.URL, d.maxRetries)
		}(i, file)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			log.Printf("Download error: %v", err)
		}
	}

	if len(failedURLs) > 0 {
		log.Printf("Failed to download the following files: %v", failedURLs)
		return fmt.Errorf("failed to download %d files", len(failedURLs))
	}
	log.Println("All files downloaded successfully")
	return nil
}

// downloadWithProxy выполняет загрузку через указанный прокси.
func (d *Downloader) downloadWithProxy(ctx context.Context, fileURL, proxyURLStr string) error {
	proxyURL, err := url.Parse(proxyURLStr)
	if err != nil {
		return fmt.Errorf("invalid proxy URL %s: %w", proxyURLStr, err)
	}

	// Используем proxy.FromURL для socks4 и socks5
	dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
	if err != nil {
		return fmt.Errorf("failed to create proxy %s: %w", proxyURLStr, err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: dialer.Dial,
		},
		Timeout: 60 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to GET %s with proxy %s: %w", fileURL, proxyURLStr, err)
	}
	defer resp.Body.Close()

	log.Printf("Response status for %s: %d", fileURL, resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code for %s: %d", fileURL, resp.StatusCode)
	}

	// Формируем путь сохранения
	relativePath := strings.TrimPrefix(fileURL, d.BaseURL+"/")
	outputPath := filepath.Join(d.outputDir, relativePath)
	log.Printf("Saving file to %s", outputPath)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	// Сохраняем файл
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("Wrote %d bytes to %s", n, outputPath)

	// Проверяем, что файл является Zip
	if err := CheckZipFile(outputPath); err != nil {
		log.Printf("Invalid Zip file %s: %v", outputPath, err)
		os.Remove(outputPath)
		return err
	}

	return nil
}

// CheckZipFile проверяет, является ли файл валидным Zip.
func CheckZipFile(path string) error {
	// Проверяем размер файла
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", path, err)
	}
	if fileInfo.Size() == 0 {
		log.Printf("Skipping empty file %s (0 bytes)", path)
		return nil
	}

	r, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	r.Close()
	return nil
}
