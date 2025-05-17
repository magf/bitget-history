package export

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // Драйвер SQLite
)

// AppendTickToOHLC добавляет тиковые данные в OHLC-файл с заданным таймфреймом.
func AppendTickToOHLC(tickData, csvPath, timeframe string, mu *sync.RWMutex) error {
	// Парсим тиковые данные: timestamp,ask_price,bid_price,ask_volume,bid_volume
	parts := strings.Split(tickData, ",")
	if len(parts) < 5 {
		return fmt.Errorf("invalid tick data: %s", tickData)
	}
	timestamp, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp in tick data: %s", parts[0])
	}
	askPrice, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return fmt.Errorf("invalid ask_price in tick data: %s", parts[1])
	}
	bidPrice, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	if err != nil {
		return fmt.Errorf("invalid bid_price in tick data: %s", parts[2])
	}
	askVolume, err := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
	if err != nil {
		return fmt.Errorf("invalid ask_volume in tick data: %s", parts[3])
	}
	bidVolume, err := strconv.ParseFloat(strings.TrimSpace(parts[4]), 64)
	if err != nil {
		return fmt.Errorf("invalid bid_volume in tick data: %s", parts[4])
	}
	midPrice := (askPrice + bidPrice) / 2.0
	volume := askVolume + bidVolume
	tickTime := time.Unix(timestamp, 0)

	// Определяем интервал свечи
	var candleDuration time.Duration
	switch timeframe {
	case "m1":
		candleDuration = time.Minute
	case "m5":
		candleDuration = 5 * time.Minute
	case "m15":
		candleDuration = 15 * time.Minute
	case "m30":
		candleDuration = 30 * time.Minute
	case "h1":
		candleDuration = time.Hour
	case "h4":
		candleDuration = 4 * time.Hour
	case "d1":
		candleDuration = 24 * time.Hour
	default:
		return fmt.Errorf("unsupported timeframe: %s", timeframe)
	}

	// Вычисляем начало свечи
	candleStart := tickTime.Truncate(candleDuration)
	candleKey := candleStart.Format("2006.01.02 15:04")

	// Структура для свечи
	type candle struct {
		Date, Time                     string
		Open, High, Low, Close, Volume float64
		Timestamp                      int64
	}

	// Читаем существующие свечи
	mu.Lock()
	defer mu.Unlock()

	var candles []candle
	fileExists := true
	f, err := os.Open(csvPath)
	if os.IsNotExist(err) {
		fileExists = false
	} else if err != nil {
		return fmt.Errorf("failed to open CSV %s: %v", csvPath, err)
	}
	if fileExists {
		defer f.Close()
		reader := csv.NewReader(f)
		_, err := reader.Read() // Пропускаем заголовок
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read header from %s: %v", csvPath, err)
		}
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading %s: %v", csvPath, err)
				continue
			}
			if len(row) < 7 {
				continue
			}
			open, _ := strconv.ParseFloat(row[2], 64)
			high, _ := strconv.ParseFloat(row[3], 64)
			low, _ := strconv.ParseFloat(row[4], 64)
			closePrice, _ := strconv.ParseFloat(row[5], 64)
			volume, _ := strconv.ParseFloat(row[6], 64)
			dateTimeStr := row[0] + " " + strings.TrimSuffix(row[1], ":00")
			ts, _ := time.Parse("2006.01.02 15:04", dateTimeStr)
			candles = append(candles, candle{
				Date:      row[0],
				Time:      row[1],
				Open:      open,
				High:      high,
				Low:       low,
				Close:     closePrice,
				Volume:    volume,
				Timestamp: ts.Unix(),
			})
		}
	}

	// Ищем свечу
	candleIndex := -1
	for i, c := range candles {
		if c.Date+" "+strings.TrimSuffix(c.Time, ":00") == candleKey {
			candleIndex = i
			break
		}
	}

	// Обновляем или создаём свечу
	var prevClose float64
	if candleIndex > 0 {
		prevClose = candles[candleIndex-1].Close
	} else if candleIndex == 0 && len(candles) > 1 {
		// Для первой свечи ищем предыдущую
		for _, c := range candles[1:] {
			if c.Timestamp < candleStart.Unix() {
				prevClose = c.Close
			}
		}
	}

	if candleIndex >= 0 {
		// Обновляем свечу
		c := &candles[candleIndex]
		if c.Open == 0 {
			c.Open = prevClose
			if c.Open == 0 {
				c.Open = midPrice
			}
		}
		c.High = max(c.High, midPrice)
		c.Low = min(c.Low, midPrice)
		if c.Low == 0 {
			c.Low = midPrice
		}
		c.Close = midPrice
		c.Volume += volume
	} else {
		// Создаём новую свечу
		openPrice := prevClose
		if openPrice == 0 {
			openPrice = midPrice
		}
		newCandle := candle{
			Date:      candleStart.Format("2006.01.02"),
			Time:      candleStart.Format("15:04:00"),
			Open:      openPrice,
			High:      midPrice,
			Low:       midPrice,
			Close:     midPrice,
			Volume:    volume,
			Timestamp: candleStart.Unix(),
		}
		candles = append(candles, newCandle)
		candleIndex = len(candles) - 1
	}

	// Обновляем следующую свечу, если она есть
	if candleIndex+1 < len(candles) {
		nextCandle := &candles[candleIndex+1]
		nextCandle.Open = candles[candleIndex].Close
	}

	// Сортируем свечи по времени
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp < candles[j].Timestamp
	})

	// Переписываем CSV
	if err := os.MkdirAll(filepath.Dir(csvPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %v", csvPath, err)
	}
	f, err = os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV %s: %v", csvPath, err)
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write([]string{"Date", "Time", "Open", "High", "Low", "Close", "Volume"}); err != nil {
		return fmt.Errorf("failed to write header to %s: %v", csvPath, err)
	}
	for _, c := range candles {
		if err := writer.Write([]string{
			c.Date,
			c.Time,
			fmt.Sprintf("%.2f", c.Open),
			fmt.Sprintf("%.2f", c.High),
			fmt.Sprintf("%.2f", c.Low),
			fmt.Sprintf("%.2f", c.Close),
			fmt.Sprintf("%.6f", c.Volume),
		}); err != nil {
			log.Printf("Failed to write candle %s %s to %s: %v", c.Date, c.Time, csvPath, err)
		}
	}

	log.Printf("Appended tick to %s, candle %s", csvPath, candleKey)
	return nil
}

// max возвращает максимум двух чисел.
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// min возвращает минимум двух чисел.
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// ExportToMT5CSV экспортирует данные depth в CSV для MetaTrader 5.
func ExportToMT5CSV(dbPath, pair, market, timeframe string, startDate, endDate time.Time) (string, error) {
	startTotal := time.Now()

	// Проверяем существование базы
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Printf("Database %s does not exist, skipping export", dbPath)
		return "", nil
	}

	// Формируем имя файла
	startStr := startDate.Format("2006-01-02")
	endStr := endDate.Format("2006-01-02")
	marketName := "spot"
	if market == "2" {
		marketName = "futures"
	}
	outputFile := filepath.Join("/tmp/bitget-history/mt5", fmt.Sprintf("%s_%s_%s_%s-%s.csv", pair, marketName, timeframe, startStr, endStr))
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return "", fmt.Errorf("failed to create directory for %s: %v", outputFile, err)
	}

	// Открываем базу
	db, err := sql.Open("sqlite3", dbPath+"?mode=ro")
	if err != nil {
		return "", fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	// Настраиваем SQLite
	_, err = db.Exec("PRAGMA busy_timeout = 10000; PRAGMA cache_size = -100000; PRAGMA synchronous = OFF;")
	if err != nil {
		log.Printf("Failed to configure SQLite: %v", err)
	}

	// Проверяем таблицу
	var tableExists string
	err = db.QueryRow(fmt.Sprintf(`SELECT name FROM sqlite_master WHERE type='table' AND name='%s'`, market)).Scan(&tableExists)
	if err == sql.ErrNoRows {
		log.Printf("Table %s does not exist, skipping", market)
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("failed to check table %s: %v", market, err)
	}

	// Читаем тики
	query := fmt.Sprintf(`
		SELECT timestamp, ask_price, bid_price, ask_volume, bid_volume
		FROM "%s"
		WHERE timestamp >= ? AND timestamp <= ?
		ORDER BY timestamp;
	`, market)
	rows, err := db.Query(query, startDate.Unix(), endDate.Unix())
	if err != nil {
		return "", fmt.Errorf("failed to query table %s: %v", market, err)
	}
	defer rows.Close()

	// Мьютекс для AppendTickToOHLC (хотя в однопоточном режиме он избыточен, оставляем для универсальности)
	var mu sync.RWMutex

	// Обрабатываем тики последовательно
	ticksProcessed := 0
	hasData := false
	for rows.Next() {
		var timestamp int64
		var askPrice, bidPrice, askVolume, bidVolume float64
		if err := rows.Scan(&timestamp, &askPrice, &bidPrice, &askVolume, &bidVolume); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}
		tickData := fmt.Sprintf("%d,%.2f,%.2f,%.6f,%.6f", timestamp, askPrice, bidPrice, askVolume, bidVolume)
		if err := AppendTickToOHLC(tickData, outputFile, timeframe, &mu); err != nil {
			log.Printf("Failed to append tick %d: %v", timestamp, err)
			continue
		}
		ticksProcessed++
		hasData = true
		if ticksProcessed%1000 == 0 {
			log.Printf("Processed %d ticks", ticksProcessed)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating rows: %v", err)
	}

	if !hasData {
		log.Printf("No data found for table %s in %s for period %s to %s", market, dbPath, startStr, endStr)
		return "", nil
	}

	log.Printf("Export completed to %s, processed %d ticks, total time %v", outputFile, ticksProcessed, time.Since(startTotal))
	return outputFile, nil
}
