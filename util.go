package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var loggedRunErrors sync.Map

func run(timeout time.Duration, command string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, command, args...).Output()
	if err != nil {
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			key := command + "|" + err.Error()
			if _, seen := loggedRunErrors.LoadOrStore(key, struct{}{}); !seen {
				log.Printf("run %s %v: %v", command, args, err)
			}
		}
		return ""
	}
	return strings.TrimSpace(string(output))
}

func sendSSE(w http.ResponseWriter, event string, data any) {
	payload, _ := json.Marshal(data)
	fmt.Fprintf(w, "event: %s\n", event)
	fmt.Fprintf(w, "data: %s\n\n", payload)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(value)
}

func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func parseUint(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimSuffix(value, ".")), 10, 64)
	return parsed
}

func parseFloat(value string) float64 {
	parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed
}

func sumCPU(value cpuTimes) uint64 {
	return value.user + value.nice + value.sys + value.idle + value.wait + value.irq + value.soft
}

func percent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(used) / float64(total)) * 100
}

func clamp(value, min, max float64) float64 {
	return math.Min(math.Max(value, min), max)
}

func formatBytes(bytes uint64) string {
	if bytes == 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	index := int(math.Min(math.Floor(math.Log(float64(bytes))/math.Log(1024)), float64(len(units)-1)))
	value := float64(bytes) / math.Pow(1024, float64(index))
	if index == 0 {
		return fmt.Sprintf("%.0f %s", value, units[index])
	}
	return fmt.Sprintf("%.1f %s", value, units[index])
}
