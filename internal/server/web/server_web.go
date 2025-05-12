package web

import (
	"net/http"
	"path/filepath"
)

// StartServer настраивает веб-сервер для раздачи статических файлов.
func StartServer(mux *http.ServeMux) {
	// Раздаём статические файлы из internal/server/web/static
	staticDir := http.Dir(filepath.Join("internal", "server", "web", "static"))
	fs := http.FileServer(staticDir)
	mux.Handle("/", fs)
}
