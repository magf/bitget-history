package downloader

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/magf/bitget-history/internal/proxymanager"
	"golang.org/x/net/proxy"
)

// Downloader управляет загрузкой файлов через прокси.
type Downloader struct {
	baseURL    string
	userAgent  string
	proxies    []string
	outputDir  string
	maxRetries int
}

// NewDownloader создаёт новый менеджер загрузки.
func NewDownloader(baseURL, userAgent, outputDir string, pm *proxymanager.ProxyManager) (*Downloader, error) {
	proxies, err := pm.GetProxies()
	if err != nil {
		return nil, fmt.Errorf("failed to get proxies: %w", err)
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("no working proxies available")
	}

	return &Downloader{
		baseURL:    baseURL,
		userAgent:  userAgent,
		proxies:    proxies,
		outputDir:  outputDir,
		maxRetries: 3,
	}, nil
}

// DownloadFiles загружает список файлов многопоточно.
func (d *Downloader) DownloadFiles(ctx context.Context, urls []string) error {
	var wg sync.WaitGroup
	results := make(chan error, len(urls))
	proxyIndex := 0
	mu := sync.Mutex{}

	log.Printf("Starting download of %d files", len(urls))
	for i, u := range urls {
		wg.Add(1)
		go func(index int, fileURL string) {
			defer wg.Done()
			log.Printf("Downloading file %d: %s", index+1, fileURL)
			err := d.downloadFile(ctx, fileURL, proxyIndex)
			if err != nil {
				results <- fmt.Errorf("failed to download %s: %w", fileURL, err)
				return
			}
			results <- nil
			mu.Lock()
			proxyIndex = (proxyIndex + 1) % len(d.proxies)
			mu.Unlock()
		}(i, u)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var errors []error
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors: %v", len(errors), errors)
	}
	log.Println("All files downloaded successfully")
	return nil
}

// downloadFile загружает один файл с повторными попытками.
func (d *Downloader) downloadFile(ctx context.Context, fileURL string, proxyIndex int) error {
	var lastErr error
	for attempt := 0; attempt < d.maxRetries; attempt++ {
		proxyURL := d.proxies[proxyIndex]
		log.Printf("Attempt %d/%d for %s using proxy %s", attempt+1, d.maxRetries, fileURL, proxyURL)
		err := d.downloadWithProxy(ctx, fileURL, proxyURL)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Failed attempt %d: %v", attempt+1, err)
		proxyIndex = (proxyIndex + 1) % len(d.proxies)
	}
	return fmt.Errorf("failed after %d retries: %w", d.maxRetries, lastErr)
}

// downloadWithProxy выполняет загрузку через указанный прокси.
func (d *Downloader) downloadWithProxy(ctx context.Context, fileURL, proxyURL string) error {
	parsedProxyURL, err := url.Parse(proxyURL)
	if err != nil {
		return fmt.Errorf("invalid proxy URL %s: %w", proxyURL, err)
	}
	dialer, err := proxy.FromURL(parsedProxyURL, proxy.Direct)
	if err != nil {
		return fmt.Errorf("failed to create proxy %s: %w", proxyURL, err)
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

	// Формируем путь сохранения, сохраняя структуру REMOTE_PATH
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

// checkZipFile проверяет, является ли файл валидным Zip-архивом.
func checkZipFile(path string) error {
	r, err := zip.OpenReader(path)
	if err != nil {
		return fmt.Errorf("not a valid Zip file: %w", err)
	}
	r.Close()
	return nil
}
