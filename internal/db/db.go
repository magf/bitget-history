package db

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3" // Драйвер SQLite
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
			volume_quote REAL, -- Мапится на volume(quote) из CSV
			size_base REAL     -- Мапится на size(base) из CSV
		)
	`)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create trades table in %s: %w", dbPath, err)
	}

	// Создаём таблицу depth (для будущего)
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

	// Не удаляем файлы в /tmp после обработки (для разработки)
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

	// Ищем CSV-файл (trades.csv)
	var csvFile *zip.File
	for _, f := range zipReader.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			csvFile = f
			break
		}
	}
	if csvFile == nil {
		return fmt.Errorf("no CSV file found in %s", zipPath)
	}

	// Извлекаем CSV
	csvReader, err := csvFile.Open()
	if err != nil {
		return fmt.Errorf("failed to open CSV in %s: %w", zipPath, err)
	}
	defer csvReader.Close()

	// Читаем CSV
	reader := csv.NewReader(csvReader)
	reader.FieldsPerRecord = -1 // Разрешить разное количество полей
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV in %s: %w", zipPath, err)
	}

	// Подготавливаем транзакцию
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

	// Конвертируем и вставляем записи
	for i, record := range records {
		if i == 0 {
			continue // Пропускаем заголовок
		}
		if len(record) < 6 {
			log.Printf("Skipping invalid record in %s at line %d: %v", zipPath, i+1, record)
			continue
		}

		// trade_id: TEXT
		tradeID := strings.TrimSpace(record[0])
		if tradeID == "" {
			log.Printf("Skipping record in %s at line %d: empty trade_id", zipPath, i+1)
			continue
		}

		// timestamp: INTEGER (Unix ms from CSV)
		timestampStr := strings.TrimSpace(record[1])
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid timestamp %s", zipPath, i+1, timestampStr)
			continue
		}

		// price: REAL
		priceStr := strings.TrimSpace(record[2])
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid price %s", zipPath, i+1, priceStr)
			continue
		}

		// side: TEXT (buy/sell)
		side := strings.TrimSpace(record[3])
		if side != "buy" && side != "sell" {
			log.Printf("Skipping record in %s at line %d: invalid side %s", zipPath, i+1, side)
			continue
		}

		// volume_quote: REAL (маппинг volume(quote) из CSV)
		volumeQuoteStr := strings.TrimSpace(record[4])
		volumeQuote, err := strconv.ParseFloat(volumeQuoteStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid volume_quote %s", zipPath, i+1, volumeQuoteStr)
			continue
		}

		// size_base: REAL (маппинг size(base) из CSV)
		sizeBaseStr := strings.TrimSpace(record[5])
		sizeBase, err := strconv.ParseFloat(sizeBaseStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid size_base %s", zipPath, i+1, sizeBaseStr)
			continue
		}

		// Вставляем запись
		_, err = stmt.Exec(tradeID, timestamp, price, side, volumeQuote, sizeBase)
		if err != nil {
			log.Printf("Failed to insert record in %s at line %d: %v", zipPath, i+1, err)
			continue
		}
	}

	// Завершаем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %w", db.path, err)
	}

	log.Printf("Successfully processed %s into %s", zipPath, db.path)
	return nil
}
