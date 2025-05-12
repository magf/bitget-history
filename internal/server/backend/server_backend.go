package backend

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

// DepthHandler обрабатывает запросы к данным depth.
func DepthHandler(w http.ResponseWriter, r *http.Request) {
	// Получаем параметры
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	table := r.URL.Query().Get("table")
	dbPath := "/var/lib/bitget-history/database/depth/BTCUSDT.db"

	if table == "" {
		table = "2" // По умолчанию futures
	}
	if start == "" || end == "" {
		log.Printf("Missing start or end parameter")
		http.Error(w, "Missing start or end parameter", http.StatusBadRequest)
		return
	}

	startTs, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		log.Printf("Invalid start parameter: %v", err)
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}
	endTs, err := strconv.ParseInt(end, 10, 64)
	if err != nil {
		log.Printf("Invalid end parameter: %v", err)
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	// Проверяем существование базы
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Printf("Database file does not exist: %s", dbPath)
		http.Error(w, fmt.Sprintf("Database file does not exist: %s", dbPath), http.StatusInternalServerError)
		return
	}

	// Открываем базу
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Printf("Failed to open database: %v", err)
		http.Error(w, fmt.Sprintf("Failed to open database: %v", err), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Проверяем существование таблицы
	var tableExists string
	err = db.QueryRow(fmt.Sprintf(`SELECT name FROM sqlite_master WHERE type='table' AND name="%s"`, table)).Scan(&tableExists)
	if err == sql.ErrNoRows {
		log.Printf("Table %s does not exist", table)
		http.Error(w, fmt.Sprintf("Table %s does not exist", table), http.StatusBadRequest)
		return
	} else if err != nil {
		log.Printf("Failed to check table existence: %v", err)
		http.Error(w, fmt.Sprintf("Failed to check table: %v", err), http.StatusInternalServerError)
		return
	}

	// Запрашиваем данные
	rows, err := db.Query(fmt.Sprintf(`SELECT timestamp, ask_price, bid_price, ask_volume, bid_volume 
		FROM "%s" WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp`, table), startTs, endTs)
	if err != nil {
		log.Printf("Failed to query database: %v", err)
		http.Error(w, fmt.Sprintf("Failed to query database: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Формируем JSON
	type DepthRecord struct {
		Timestamp int64   `json:"timestamp"`
		AskPrice  float64 `json:"ask_price"`
		BidPrice  float64 `json:"bid_price"`
		AskVolume float64 `json:"ask_volume"`
		BidVolume float64 `json:"bid_volume"`
	}

	var records []DepthRecord
	for rows.Next() {
		var rec DepthRecord
		if err := rows.Scan(&rec.Timestamp, &rec.AskPrice, &rec.BidPrice, &rec.AskVolume, &rec.BidVolume); err != nil {
			log.Printf("Failed to scan row: %v", err)
			http.Error(w, fmt.Sprintf("Failed to scan row: %v", err), http.StatusInternalServerError)
			return
		}
		records = append(records, rec)
	}

	// Отправляем JSON
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(records)
}

// StartServer запускает сервер с endpoint'ом /depth.
func StartServer(mux *http.ServeMux) {
	mux.HandleFunc("/depth", DepthHandler)
}
