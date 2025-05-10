package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// ProxyManager управляет списком прокси.
type ProxyManager struct {
	rawFile     string
	workingFile string
	fallback    string
}

// NewProxyManager создаёт новый менеджер прокси.
func NewProxyManager(rawFile, workingFile, fallback string) (*ProxyManager, error) {
	return &ProxyManager{
		rawFile:     rawFile,
		workingFile: workingFile,
		fallback:    fallback,
	}, nil
}

// EnsureProxies загружает или проверяет список прокси.
func (pm *ProxyManager) EnsureProxies(ctx context.Context) error {
	// Проверяем наличие rawFile
	if err := pm.downloadProxies(ctx); err != nil {
		return fmt.Errorf("failed to download proxies: %w", err)
	}

	// Читаем сырые прокси
	proxies, err := pm.loadProxies(pm.rawFile)
	if err != nil {
		return fmt.Errorf("failed to load proxies: %w", err)
	}
	if len(proxies) == 0 {
		return fmt.Errorf("proxy list is empty: %s", pm.rawFile)
	}

	// Проверяем прокси многопоточно
	workingProxies, err := pm.checkProxies(ctx, proxies)
	if err != nil {
		return fmt.Errorf("failed to check proxies: %w", err)
	}
	if len(workingProxies) == 0 {
		return fmt.Errorf("no working proxies found")
	}

	// Сохраняем рабочие прокси
	if err := pm.saveProxies(workingProxies); err != nil {
		return fmt.Errorf("failed to save proxies: %w", err)
	}
	return nil
}

// downloadProxies скачивает список прокси, если файл отсутствует.
func (pm *ProxyManager) downloadProxies(ctx context.Context) error {
	if _, err := os.Stat(pm.rawFile); err == nil {
		return nil // Файл существует
	}

	// Создаём директорию
	if err := os.MkdirAll(filepath.Dir(pm.rawFile), 0755); err != nil {
		return err
	}

	// Создаём временный файл
	f, err := os.Create(pm.rawFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Настраиваем HTTP-клиент
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	if pm.fallback != "" {
		proxyURL, err := url.Parse(pm.fallback)
		if err != nil {
			return fmt.Errorf("invalid fallback proxy URL: %w", err)
		}
		dialer, err := proxy.FromURL(proxyURL, proxy.Direct)
		if err != nil {
			return fmt.Errorf("failed to create fallback proxy: %w", err)
		}
		client.Transport = &http.Transport{
			Dial: dialer.Dial,
		}
	}

	// Скачиваем списки для SOCKS4 и SOCKS5
	for _, proto := range []string{"4", "5"} {
		url := fmt.Sprintf("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks%s/data.txt", proto)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to download %s: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code for %s: %d", url, resp.StatusCode)
		}

		_, err = io.Copy(f, resp.Body)
		if err != nil {
			return err
		}
		_, err = f.WriteString("\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// loadProxies загружает список прокси из файла.
func (pm *ProxyManager) loadProxies(file string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var proxies []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			proxies = append(proxies, line)
		}
	}
	return proxies, scanner.Err()
}

// checkProxies проверяет прокси многопоточно.
func (pm *ProxyManager) checkProxies(ctx context.Context, proxies []string) ([]string, error) {
	var wg sync.WaitGroup
	results := make(chan string, len(proxies))
	var mu sync.Mutex
	var workingProxies []string

	// Запускаем goroutine для каждого прокси
	for _, p := range proxies {
		wg.Add(1)
		go func(proxyURL string) {
			defer wg.Done()
			ok, err := checkProxy(ctx, proxyURL)
			if err != nil {
				return
			}
			if ok {
				results <- proxyURL
			}
		}(p)
	}

	// Собираем результаты в отдельной goroutine
	go func() {
		for p := range results {
			mu.Lock()
			workingProxies = append(workingProxies, p)
			mu.Unlock()
		}
	}()

	// Ждём завершения всех проверок
	wg.Wait()
	close(results)

	return workingProxies, nil
}

// checkProxy проверяет работоспособность одного прокси.
func checkProxy(ctx context.Context, proxyURL string) (bool, error) {
	proxyURL = strings.Replace(proxyURL, "socks4://", "socks5://", 1) // Унифицируем для SOCKS5
	parsedURL, err := url.Parse(proxyURL)
	if err != nil {
		return false, nil // Игнорируем невалидные URL
	}
	dialer, err := proxy.FromURL(parsedURL, proxy.Direct)
	if err != nil {
		return false, nil // Игнорируем невалидные прокси
	}

	transport := &http.Transport{
		Dial: dialer.Dial,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", "https://ifconfig.io", nil)
	if err != nil {
		return false, nil
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()

	// Проверяем, что IP совпадает с прокси
	proxyIP := strings.Split(strings.TrimPrefix(proxyURL, "socks5://"), ":")[0]
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, nil
	}
	return strings.TrimSpace(string(body)) == proxyIP, nil
}

// saveProxies сохраняет рабочие прокси в файл.
func (pm *ProxyManager) saveProxies(proxies []string) error {
	// Создаём директорию
	if err := os.MkdirAll(filepath.Dir(pm.workingFile), 0755); err != nil {
		return err
	}

	f, err := os.Create(pm.workingFile)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, p := range proxies {
		if _, err := f.WriteString(p + "\n"); err != nil {
			return err
		}
	}
	return nil
}

// GetProxies возвращает список рабочих прокси для потоков.
func (pm *ProxyManager) GetProxies() ([]string, error) {
	return pm.loadProxies(pm.workingFile)
}
