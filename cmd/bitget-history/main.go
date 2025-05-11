package db

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3" // Драйвер SQLite
	"github.com/tealeg/xlsx/v3"
)

// DB управляет подключением к SQLite и выгрузкой данных.
type DB struct {
	conn *sql.DB
	path string // Для логирования
}

// NewDB создаёт новое подключение к SQLite и инициализирует схему.
func NewDB(dbPath string) (*DB, error) {
	// Проверяем, что путь не содержит шаблонов
	if strings.Contains(dbPath, "%s") {
		return nil, fmt.Errorf("invalid database path: %s contains placeholder %%s", dbPath)
	}
	log.Printf("Opening database: %s", dbPath)
	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", dbPath, err)
	}

	// Включаем WAL для производительности
	_, err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set WAL mode for %s: %w", dbPath, err)
	}

	// Создаём таблицу trades
	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS trades (
			trade_id TEXT PRIMARY KEY,
			timestamp INTEGER,
			price REAL,
			side TEXT,
			volume_quote REAL,
			size_base REAL
		)
	`)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create trades table in %s: %w", dbPath, err)
	}

	// Создаём таблицу depth
	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS depth (
			timestamp INTEGER PRIMARY KEY,
			ask_price REAL,
			bid_price REAL,
			ask_volume REAL,
			bid_volume REAL
		)
	`)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create depth table in %s: %w", dbPath, err)
	}

	// Создаём индексы
	_, err = conn.Exec("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create index idx_trades_timestamp in %s: %w", dbPath, err)
	}
	_, err = conn.Exec("CREATE INDEX IF NOT EXISTS idx_depth_timestamp ON depth(timestamp)")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create index idx_depth_timestamp in %s: %w", dbPath, err)
	}

	return &DB{conn: conn, path: dbPath}, nil
}

// Close закрывает подключение к базе.
func (db *DB) Close() error {
	log.Printf("Closing database: %s", db.path)
	return db.conn.Close()
}

// ProcessZipFiles обрабатывает Zip-файлы и выгружает данные в SQLite.
func (db *DB) ProcessZipFiles(zipFiles []string) error {
	tmpDir := "/tmp/bitget-history"
	// Очищаем /tmp/bitget-history перед началом
	log.Printf("Cleaning temporary directory: %s", tmpDir)
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("failed to clean %s: %w", tmpDir, err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create %s: %w", tmpDir, err)
	}

	for _, zipPath := range zipFiles {
		log.Printf("Processing zip file: %s", zipPath)
		if err := db.processSingleZip(zipPath, tmpDir); err != nil {
			log.Printf("Failed to process %s: %v", zipPath, err)
			continue // Продолжаем с другими файлами
		}
	}

	log.Printf("Temporary files left in %s for debugging", tmpDir)
	return nil
}

// processSingleZip обрабатывает один Zip-файл.
func (db *DB) processSingleZip(zipPath, tmpDir string) error {
	// Открываем Zip
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip %s: %w", zipPath, err)
	}
	defer zipReader.Close()

	// Проверяем файлы в Zip
	for _, f := range zipReader.File {
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("corrupted zip %s: failed to open file %s: %w", zipPath, f.Name, err)
		}
		rc.Close()
	}

	// Ищем CSV или XLSX
	var csvFile *zip.File
	var xlsxFile *zip.File
	for _, f := range zipReader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			csvFile = f
			break
		}
		if strings.HasSuffix(strings.ToLower(f.Name), ".xlsx") {
			xlsxFile = f
			break
		}
	}

	// Формируем путь для CSV
	zipBase := filepath.Base(zipPath)             // Например, "20250502.zip"
	zipBase = strings.TrimSuffix(zipBase, ".zip") // "20250502"
	pathParts := strings.Split(zipPath, string(os.PathSeparator))
	marketCode := "unknown"
	for i, part := range pathParts {
		if part == "BTCUSDT" && i+1 < len(pathParts) {
			marketCode = pathParts[i+1] // "1" или "2"
			break
		}
	}
	csvFileName := fmt.Sprintf("%s_%s.csv", marketCode, zipBase)
	csvPath := filepath.Join(tmpDir, csvFileName)

	// Если CSV найден, извлекаем его
	if csvFile != nil {
		if err := extractFile(csvFile, csvPath); err != nil {
			return fmt.Errorf("failed to extract CSV from %s: %w", zipPath, err)
		}
		log.Printf("Extracted CSV: %s", csvPath)
	} else if xlsxFile != nil {
		// Извлекаем XLSX
		xlsxPath := filepath.Join(tmpDir, xlsxFile.Name)
		if err := extractFile(xlsxFile, xlsxPath); err != nil {
			return fmt.Errorf("failed to extract XLSX from %s: %w", zipPath, err)
		}
		// Конвертируем XLSX в CSV
		if err := convertXLSXtoCSV(xlsxPath, csvPath); err != nil {
			return fmt.Errorf("failed to convert XLSX to CSV for %s: %w", zipPath, err)
		}
		log.Printf("Converted XLSX to CSV: %s", csvPath)
	} else {
		return fmt.Errorf("no CSV file found in %s (and no XLSX to convert)", zipPath)
	}

	// Определяем тип данных по пути
	isDepth := strings.Contains(strings.ToLower(zipPath), "/depth/")

	// Обрабатываем CSV
	if isDepth {
		return db.processDepthCSV(zipPath, csvPath)
	}
	return db.processTradesCSV(zipPath, csvPath)
}

// extractFile извлекает файл из Zip в указанный путь.
func extractFile(file *zip.File, destPath string) error {
	fileReader, err := file.Open()
	if err != nil {
		return err
	}
	defer fileReader.Close()

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}

	outFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, fileReader)
	return err
}

// convertXLSXtoCSV конвертирует XLSX в CSV.
func convertXLSXtoCSV(xlsxPath, csvPath string) error {
	xlsxFile, err := xlsx.OpenFile(xlsxPath)
	if err != nil {
		return fmt.Errorf("failed to open XLSX %s: %w", xlsxPath, err)
	}

	if len(xlsxFile.Sheets) == 0 {
		return fmt.Errorf("no sheets found in XLSX %s", xlsxPath)
	}

	// Берем первый лист и конвертируем в CSV
	sheet := xlsxFile.Sheets[0]
	return sheet.ToCsv(csvPath)
}

// processTradesCSV обрабатывает CSV для trades.
func (db *DB) processTradesCSV(zipPath, csvPath string) error {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV %s: %w", csvPath, err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1 // Разрешить разное количество полей
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV %s: %w", csvPath, err)
	}

	log.Printf("Processed %d rows from CSV: %s", len(records)-1, csvPath)

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %w", db.path, err)
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO trades (trade_id, timestamp, price, side, volume_quote, size_base) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement in %s: %w", db.path, err)
	}
	defer stmt.Close()

	for i, record := range records {
		if i == 0 {
			continue // Пропускаем заголовок
		}
		if len(record) < 6 {
			log.Printf("Skipping invalid record in %s at line %d: %v", zipPath, i+1, record)
			continue
		}

		tradeID := strings.TrimSpace(record[0])
		if tradeID == "" {
			log.Printf("Skipping record in %s at line %d: empty trade_id", zipPath, i+1)
			continue
		}

		timestampStr := strings.TrimSpace(record[1])
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid timestamp %s", zipPath, i+1, timestampStr)
			continue
		}

		priceStr := strings.TrimSpace(record[2])
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid price %s", zipPath, i+1, priceStr)
			continue
		}

		side := strings.TrimSpace(record[3])
		if side != "buy" && side != "sell" {
			log.Printf("Skipping record in %s at line %d: invalid side %s", zipPath, i+1, side)
			continue
		}

		volumeQuoteStr := strings.TrimSpace(record[4])
		volumeQuote, err := strconv.ParseFloat(volumeQuoteStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid volume_quote %s", zipPath, i+1, volumeQuoteStr)
			continue
		}

		sizeBaseStr := strings.TrimSpace(record[5])
		sizeBase, err := strconv.ParseFloat(sizeBaseStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid size_base %s", zipPath, i+1, sizeBaseStr)
			continue
		}

		_, err = stmt.Exec(tradeID, timestamp, price, side, volumeQuote, sizeBase)
		if err != nil {
			log.Printf("Failed to insert record in %s at line %d: %v", zipPath, i+1, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %w", db.path, err)
	}

	log.Printf("Successfully processed trades CSV from %s into %s", zipPath, db.path)
	return nil
}

// processDepthCSV обрабатывает CSV для depth.
func (db *DB) processDepthCSV(zipPath, csvPath string) error {
	csvFile, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV %s: %w", csvPath, err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = -1 // Разрешить разное количество полей
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV %s: %w", csvPath, err)
	}

	log.Printf("Processed %d rows from CSV: %s", len(records)-1, csvPath)

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %w", db.path, err)
	}
	stmt, err := tx.Prepare("INSERT OR IGNORE INTO depth (timestamp, ask_price, bid_price, ask_volume, bid_volume) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement in %s: %w", db.path, err)
	}
	defer stmt.Close()

	for i, record := range records {
		if i == 0 {
			continue // Пропускаем заголовок
		}

		for len(record) < 5 {
			record = append(record, "0.0")
		}

		timestampStr := strings.TrimSpace(record[0])
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid timestamp %s: %v", zipPath, i+1, timestampStr, record)
			continue
		}

		askPriceStr := strings.TrimSpace(record[1])
		askPrice, err := strconv.ParseFloat(askPriceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid ask_price %s: %v", zipPath, i+1, askPriceStr, record)
			continue
		}

		bidPriceStr := strings.TrimSpace(record[2])
		bidPrice, err := strconv.ParseFloat(bidPriceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid bid_price %s: %v", zipPath, i+1, bidPriceStr, record)
			continue
		}

		askVolumeStr := strings.TrimSpace(record[3])
		askVolume, err := strconv.ParseFloat(askVolumeStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid ask_volume %s: %v", zipPath, i+1, askVolumeStr, record)
			continue
		}

		bidVolumeStr := strings.TrimSpace(record[4])
		bidVolume, err := strconv.ParseFloat(bidVolumeStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid bid_volume %s: %v", zipPath, i+1, bidVolumeStr, record)
			continue
		}

		_, err = stmt.Exec(timestamp, askPrice, bidPrice, askVolume, bidVolume)
		if err != nil {
			log.Printf("Failed to insert record in %s at line %d: %v", zipPath, i+1, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %w", db.path, err)
	}

	log.Printf("Successfully processed depth CSV from %s into %s", zipPath, db.path)
	return nil
}
