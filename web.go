package coordination

import (
	_ "embed"
	"net/http"
)

//go:embed web/console.html
var consoleHTML []byte

// ConsoleMarker is the marker the runtime probe verifies in the served page.
const ConsoleMarker = "coordination-console"

// ServeConsole writes the management console page.
func ServeConsole(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(consoleHTML)
}
