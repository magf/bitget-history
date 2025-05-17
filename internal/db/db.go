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
	conn     *sql.DB
	path     string // Для логирования
	dataType string // trades или depth
}

// NewDB создаёт новое подключение к SQLite и инициализирует схему.
func NewDB(TempDbPath, dataType string) (*DB, error) {
	// Проверяем, что путь не содержит шаблонов
	if strings.Contains(TempDbPath, "%s") {
		return nil, fmt.Errorf("invalid database path: %s contains placeholder %%s", TempDbPath)
	}
	if dataType != "trades" && dataType != "depth" {
		return nil, fmt.Errorf("invalid data type: %s (must be trades or depth)", dataType)
	}
	log.Printf("Opening database: %s for %s", TempDbPath, dataType)
	conn, err := sql.Open("sqlite3", TempDbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", TempDbPath, err)
	}

	// Включаем WAL для производительности
	_, err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set WAL mode for %s: %w", TempDbPath, err)
	}

	if dataType == "trades" {
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
			return nil, fmt.Errorf("failed to create trades table in %s: %w", TempDbPath, err)
		}
		log.Printf("Created trades table in %s", TempDbPath)

		// Создаём индекс
		_, err = conn.Exec("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create index idx_trades_timestamp in %s: %w", TempDbPath, err)
		}
		log.Printf("Created index idx_trades_timestamp in %s", TempDbPath)
	} else { // depth
		// Создаём таблицу 1 (spot)
		_, err = conn.Exec(`
			CREATE TABLE IF NOT EXISTS "1" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp INTEGER,
				ask_price REAL,
				bid_price REAL,
				ask_volume REAL,
				bid_volume REAL
			)
		`)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create table 1 in %s: %w", TempDbPath, err)
		}
		log.Printf("Created table 1 in %s", TempDbPath)

		// Создаём таблицу 2 (futures)
		_, err = conn.Exec(`
			CREATE TABLE IF NOT EXISTS "2" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp INTEGER,
				ask_price REAL,
				bid_price REAL,
				ask_volume REAL,
				bid_volume REAL
			)
		`)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create table 2 in %s: %w", TempDbPath, err)
		}
		log.Printf("Created table 2 in %s", TempDbPath)

		// Создаём индексы
		_, err = conn.Exec(`CREATE INDEX IF NOT EXISTS idx_1_timestamp ON "1"(timestamp)`)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create index idx_1_timestamp in %s: %w", TempDbPath, err)
		}
		log.Printf("Created index idx_1_timestamp in %s", TempDbPath)

		_, err = conn.Exec(`CREATE INDEX IF NOT EXISTS idx_2_timestamp ON "2"(timestamp)`)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create index idx_2_timestamp in %s: %w", TempDbPath, err)
		}
		log.Printf("Created index idx_2_timestamp in %s", TempDbPath)
	}

	return &DB{conn: conn, path: TempDbPath, dataType: dataType}, nil
}

// Close закрывает подключение к базе и синкает WAL.
func (db *DB) Close() error {
	log.Printf("Closing database: %s", db.path)
	if db.conn != nil {
		// Выполняем чекпоинт WAL
		_, err := db.conn.Exec("PRAGMA wal_checkpoint(FULL);")
		if err != nil {
			log.Printf("Failed to perform WAL checkpoint for %s: %v", db.path, err)
		} else {
			log.Printf("WAL checkpoint successful for %s", db.path)
		}
		err = db.conn.Close()
		db.conn = nil
		if err != nil {
			return fmt.Errorf("failed to close database %s: %w", db.path, err)
		}
	}
	log.Printf("Database %s closed successfully", db.path)
	return nil
}

// ProcessZipFiles обрабатывает Zip-файлы и выгружает данные в SQLite.
func (db *DB) ProcessZipFiles(zipFiles []string, debug bool) error {
	tmpRawDataDir := "/tmp/bitget-history/raw"
	// Очищаем /tmp/bitget-history/database
	log.Printf("Cleaning temporary directory: %s", tmpRawDataDir)
	if err := os.RemoveAll(tmpRawDataDir); err != nil {
		return fmt.Errorf("failed to clean %s: %w", tmpRawDataDir, err)
	}
	if err := os.MkdirAll(tmpRawDataDir, 0755); err != nil {
		return fmt.Errorf("failed to create %s: %w", tmpRawDataDir, err)
	}

	// Дропаем таблицы перед обработкой (depth only)
	if db.dataType == "depth" {
		log.Printf("Dropping depth tables in %s", db.path)
		_, err := db.conn.Exec(`DROP TABLE IF EXISTS "1"`)
		if err != nil {
			return fmt.Errorf("failed to drop table 1 in %s: %w", db.path, err)
		}
		_, err = db.conn.Exec(`DROP TABLE IF EXISTS "2"`)
		if err != nil {
			return fmt.Errorf("failed to drop table 2 in %s: %w", db.path, err)
		}
		// Пересоздаём таблицы
		_, err = db.conn.Exec(`
			CREATE TABLE "1" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp INTEGER,
				ask_price REAL,
				bid_price REAL,
				ask_volume REAL,
				bid_volume REAL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to recreate table 1 in %s: %w", db.path, err)
		}
		log.Printf("Recreated table 1 in %s", db.path)
		_, err = db.conn.Exec(`
			CREATE TABLE "2" (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp INTEGER,
				ask_price REAL,
				bid_price REAL,
				ask_volume REAL,
				bid_volume REAL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to recreate table 2 in %s: %w", db.path, err)
		}
		log.Printf("Recreated table 2 in %s", db.path)
		// Пересоздаём индексы
		_, err = db.conn.Exec(`CREATE INDEX idx_1_timestamp ON "1"(timestamp)`)
		if err != nil {
			return fmt.Errorf("failed to recreate index idx_1_timestamp in %s: %w", db.path, err)
		}
		log.Printf("Recreated index idx_1_timestamp in %s", db.path)
		_, err = db.conn.Exec(`CREATE INDEX idx_2_timestamp ON "2"(timestamp)`)
		if err != nil {
			return fmt.Errorf("failed to recreate index idx_2_timestamp in %s: %w", db.path, err)
		}
		log.Printf("Recreated index idx_2_timestamp in %s", db.path)
	}

	for _, zipPath := range zipFiles {
		// Проверяем размер файла
		fileInfo, err := os.Stat(zipPath)
		if err != nil {
			return fmt.Errorf("failed to stat file %s: %w", zipPath, err)
		}
		if fileInfo.Size() == 0 {
			if debug {
				log.Printf("Skipping empty file %s (0 bytes)", zipPath)
			}
			continue // Пропускаем пустой файл
		}

		if debug {
			log.Printf("Processing zip file: %s", zipPath)
		} else {
			fmt.Fprintf(os.Stdout, "\r  Processing zip file: %-70s                    \r", zipPath)
		}

		if err := db.processSingleZip(zipPath, tmpRawDataDir, debug); err != nil {
			log.Printf("Failed to process %s: %v", zipPath, err)
			continue // Продолжаем с другими файлами
		}
	}

	fmt.Fprintln(os.Stdout)
	log.Printf("Temporary files left in %s for debugging", tmpRawDataDir)
	return nil
}

// processSingleZip обрабатывает один Zip-файл.
func (db *DB) processSingleZip(zipPath, tmpRawDataDir string, debug bool) error {
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
	zipBase := filepath.Base(zipPath)             // Например, "20250502_001.zip"
	zipBase = strings.TrimSuffix(zipBase, ".zip") // "20250502_001"
	pathParts := strings.Split(zipPath, string(os.PathSeparator))
	marketCode := "unknown"
	for i, part := range pathParts {
		if part == "BTCUSDT" && i+1 < len(pathParts) {
			marketCode = pathParts[i+1] // "1", "2", "SPBL", "UMCBL"
			break
		}
	}
	csvFileName := fmt.Sprintf("%s_%s.csv", marketCode, zipBase)
	csvPath := filepath.Join(tmpRawDataDir, csvFileName)

	// Если CSV найден, извлекаем его
	if csvFile != nil {
		if err := extractFile(csvFile, csvPath); err != nil {
			return fmt.Errorf("failed to extract CSV from %s: %w", zipPath, err)
		}
		log.Printf("Extracted CSV: %s", csvPath)
	} else if xlsxFile != nil {
		// Извлекаем XLSX
		xlsxPath := filepath.Join(tmpRawDataDir, xlsxFile.Name)
		if err := extractFile(xlsxFile, xlsxPath); err != nil {
			return fmt.Errorf("failed to extract XLSX from %s: %w", zipPath, err)
		}
		// Конвертируем XLSX в CSV
		if err := convertXLSXtoCSV(xlsxPath, csvPath); err != nil {
			return fmt.Errorf("failed to convert XLSX to CSV for %s: %w", zipPath, err)
		}
		if debug {
			log.Printf("Converted XLSX to CSV: %s", csvPath)
		}
	} else {
		return fmt.Errorf("no CSV file found in %s (and no XLSX to convert)", zipPath)
	}

	// Обрабатываем CSV
	if db.dataType == "depth" {
		tableName := marketCode // "1" или "2"
		return db.processDepthCSV(zipPath, csvPath, tableName, debug)
	}
	return db.processTradesCSV(zipPath, csvPath, debug)
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
	// Читаем XLSX в трёхмерный слайс
	rows, err := xlsx.FileToSlice(xlsxPath)
	if err != nil {
		return fmt.Errorf("failed to read XLSX %s: %w", xlsxPath, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("no sheets found in XLSX %s", xlsxPath)
	}

	// Берём первый лист
	sheetRows := rows[0]
	if len(sheetRows) == 0 {
		return fmt.Errorf("no rows found in first sheet of XLSX %s", xlsxPath)
	}

	// Открываем CSV для записи
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV %s: %w", csvPath, err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Пишем заголовок в зависимости от типа данных
	isDepth := strings.Contains(strings.ToLower(xlsxPath), "depth")
	var header []string
	numColumns := 5
	if isDepth {
		header = []string{"timestamp", "ask_price", "bid_price", "ask_volume", "bid_volume"}
	} else {
		header = []string{"trade_id", "timestamp", "price", "side", "volume_quote", "size_base"}
		numColumns = 6
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header to %s: %w", csvPath, err)
	}

	// Обрабатываем строки (пропускаем заголовок)
	for rowIdx, row := range sheetRows {
		if rowIdx == 0 {
			continue // Пропускаем заголовок
		}

		// Убедимся, что строка имеет достаточно столбцов
		for len(row) < numColumns {
			row = append(row, "")
		}

		// Подготавливаем запись
		record := make([]string, numColumns)
		for colIdx := 0; colIdx < numColumns; colIdx++ {
			cellValue := strings.TrimSpace(row[colIdx])
			// Исправляем числовые поля
			if (isDepth && colIdx > 0) || (!isDepth && (colIdx == 2 || colIdx == 4 || colIdx == 5)) {
				if strings.HasSuffix(cellValue, ".") {
					cellValue += "0"
				}
				if cellValue == "" {
					cellValue = "0.0"
				}
			}
			record[colIdx] = cellValue
		}

		// Пропускаем пустые строки
		if strings.Join(record, "") == "" {
			continue
		}

		// Записываем строку в CSV
		if err := writer.Write(record); err != nil {
			log.Printf("Failed to write row %d to CSV %s: %v", rowIdx+1, csvPath, err)
			continue
		}
	}

	return nil
}

// processTradesCSV обрабатывает CSV для trades.
func (db *DB) processTradesCSV(zipPath, csvPath string, debug bool) error {
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

	if debug {
		log.Printf("Processed %d rows from CSV: %s", len(records)-1, csvPath)
	}

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

	inserted := 0
	skipped := 0
	for i, record := range records {
		if i == 0 {
			continue // Пропускаем заголовок
		}
		if len(record) < 6 {
			log.Printf("Skipping invalid record in %s at line %d: %v", zipPath, i+1, record)
			skipped++
			continue
		}

		tradeID := strings.TrimSpace(record[0])
		if tradeID == "" {
			log.Printf("Skipping record in %s at line %d: empty trade_id", zipPath, i+1)
			skipped++
			continue
		}

		timestampStr := strings.TrimSpace(record[1])
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid timestamp %s", zipPath, i+1, timestampStr)
			skipped++
			continue
		}

		priceStr := strings.TrimSpace(record[2])
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid price %s", zipPath, i+1, priceStr)
			skipped++
			continue
		}

		side := strings.TrimSpace(record[3])
		if side != "buy" && side != "sell" {
			log.Printf("Skipping record in %s at line %d: invalid side %s", zipPath, i+1, side)
			skipped++
			continue
		}

		volumeQuoteStr := strings.TrimSpace(record[4])
		volumeQuote, err := strconv.ParseFloat(volumeQuoteStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid volume_quote %s", zipPath, i+1, volumeQuoteStr)
			skipped++
			continue
		}

		sizeBaseStr := strings.TrimSpace(record[5])
		sizeBase, err := strconv.ParseFloat(sizeBaseStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid size_base %s", zipPath, i+1, sizeBaseStr)
			skipped++
			continue
		}

		result, err := stmt.Exec(tradeID, timestamp, price, side, volumeQuote, sizeBase)
		if err != nil {
			log.Printf("Failed to insert record in %s at line %d: %v", zipPath, i+1, err)
			skipped++
			continue
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			if debug {

				log.Printf("Skipped record in %s at line %d: duplicate trade_id %s", zipPath, i+1, tradeID)
			} else {
				fmt.Fprintf(os.Stdout, "\rSkipped record in %s at line %d: duplicate trade_id %s", zipPath, i+1, tradeID)
			}
			skipped++
		} else {
			inserted++
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction in %s: %w", db.path, err)
	}
	log.Printf("\nCommitted transaction for trades CSV %s in %s, inserted %d rows, skipped %d rows", csvPath, db.path, inserted, skipped)

	// Выполняем чекпоинт WAL
	_, err = db.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	if err != nil {
		log.Printf("Failed to perform WAL checkpoint after trades CSV %s: %v", csvPath, err)
	} else {
		if debug {
			log.Printf("WAL checkpoint successful after trades CSV %s", csvPath)
		}
	}

	return nil
}

// processDepthCSV обрабатывает CSV для depth.
func (db *DB) processDepthCSV(zipPath, csvPath, tableName string, debug bool) error {
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

	if debug {
		log.Printf("Processed %d rows from CSV: %s", len(records)-1, csvPath)
	}

	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %w", db.path, err)
	}
	stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO "%s" (timestamp, ask_price, bid_price, ask_volume, bid_volume) VALUES (?, ?, ?, ?, ?)`, tableName))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare statement for table %s in %s: %w", tableName, db.path, err)
	}
	defer stmt.Close()

	inserted := 0
	skipped := 0
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
			skipped++
			continue
		}

		askPriceStr := strings.TrimSpace(record[1])
		askPrice, err := strconv.ParseFloat(askPriceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid ask_price %s: %v", zipPath, i+1, askPriceStr, record)
			skipped++
			continue
		}

		bidPriceStr := strings.TrimSpace(record[2])
		bidPrice, err := strconv.ParseFloat(bidPriceStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid bid_price %s: %v", zipPath, i+1, bidPriceStr, record)
			skipped++
			continue
		}

		askVolumeStr := strings.TrimSpace(record[3])
		askVolume, err := strconv.ParseFloat(askVolumeStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid ask_volume %s: %v", zipPath, i+1, askVolumeStr, record)
			skipped++
			continue
		}

		bidVolumeStr := strings.TrimSpace(record[4])
		bidVolume, err := strconv.ParseFloat(bidVolumeStr, 64)
		if err != nil {
			log.Printf("Skipping record in %s at line %d: invalid bid_volume %s: %v", zipPath, i+1, bidVolumeStr, record)
			skipped++
			continue
		}

		result, err := stmt.Exec(timestamp, askPrice, bidPrice, askVolume, bidVolume)
		if err != nil {
			log.Printf("Failed to insert record in %s at line %d: %v", zipPath, i+1, err)
			skipped++
			continue
		}
		affected, _ := result.RowsAffected()
		if affected > 0 {
			inserted++
		} else {
			skipped++
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction for table %s in %s: %w", tableName, db.path, err)
	}
	if debug {
		log.Printf("Committed transaction for depth CSV %s in %s (table %s), inserted %d rows, skipped %d rows", csvPath, db.path, tableName, inserted, skipped)
	}
	// Выполняем чекпоинт WAL
	_, err = db.conn.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	if err != nil {
		log.Printf("Failed to perform WAL checkpoint after depth CSV %s (table %s): %v", csvPath, tableName, err)
	} else {
		if debug {
			log.Printf("WAL checkpoint successful after depth CSV %s (table %s)", csvPath, tableName)
		}
	}

	return nil
}
