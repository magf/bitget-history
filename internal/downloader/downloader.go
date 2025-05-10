package downloader

import (
	"archive/zip"
	"context"
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
	baseURL    string
	userAgent  string
	outputDir  string
	proxyMgr   *proxymanager.ProxyManager
	maxRetries int
}

// NewDownloader создаёт новый загрузчик.
func NewDownloader(baseURL, userAgent, outputDir string, proxyMgr *proxymanager.ProxyManager) (*Downloader, error) {
	return &Downloader{
		baseURL:    baseURL,
		userAgent:  userAgent,
		outputDir:  outputDir,
		proxyMgr:   proxyMgr,
		maxRetries: 3,
	}, nil
}

// DownloadFiles загружает файлы по списку URL-ов.
func (d *Downloader) DownloadFiles(ctx context.Context, urls []string) error {
	log.Printf("Starting download of %d files", len(urls))
	var wg sync.WaitGroup
	errChan := make(chan error, len(urls))
	badProxies := make(map[string]struct{}) // Кэш нерабочих прокси

	for i, url := range urls {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			log.Printf("Downloading file %d: %s", i+1, url)
			for attempt := 1; attempt <= 5; attempt++ { // Увеличиваем до 5 попыток
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
					log.Printf("All proxies marked as bad for %s", url)
					errChan <- fmt.Errorf("no good proxies left for %s", url)
					return
				}

				proxyIndex := rand.Intn(len(availableProxies))
				proxyURL := availableProxies[proxyIndex]
				log.Printf("Attempt %d/5 for %s using proxy %s", attempt, url, proxyURL)

				err = d.downloadWithProxy(ctx, url, proxyURL)
				if err == nil {
					return
				}
				log.Printf("Failed attempt %d for %s with proxy %s: %v", attempt, url, proxyURL, err)
				// Помечаем прокси как нерабочий при определённых ошибках
				if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "timeout") {
					badProxies[proxyURL] = struct{}{}
					log.Printf("Marked proxy %s as bad", proxyURL)
				}
				time.Sleep(time.Second * time.Duration(attempt))
			}
			errChan <- fmt.Errorf("failed to download %s after 5 attempts", url)
		}(i, url)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
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

	// Используем proxy.FromURL для socks4 и socks5 (bdandy/go-socks4 добавляет поддержку socks4)
	dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
	if err != nil {
		return fmt.Errorf("failed to create proxy %s: %w", proxyURLStr, err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: dialer.Dial,
		},
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Printf("Response status for %s: %d", fileURL, resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code for %s: %d", fileURL, resp.StatusCode)
	}

	// Формируем путь сохранения
	relativePath := strings.TrimPrefix(fileURL, d.baseURL+"/")
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
	if err := checkZipFile(outputPath); err != nil {
		log.Printf("Invalid Zip file %s: %v", outputPath, err)
		os.Remove(outputPath)
		return err
	}

	return nil
}

// checkZipFile проверяет, является ли файл валидным Zip.
func checkZipFile(path string) error {
	r, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	r.Close()
	return nil
}
