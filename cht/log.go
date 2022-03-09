package cht

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logInfo struct {
	Addr  string
	Ready bool
}

// cut field between start and end, trimming space.
//
// E.g. cut("[ 12345 ]", "[", "]") == "12345".
func cut(s, start, end string) string {
	if s == "" || start == "" || end == "" {
		return ""
	}
	left := strings.Index(s, start)
	if left < 0 {
		return ""
	}
	s = s[left+1:]
	right := strings.Index(s, end)
	if right < 0 {
		return ""
	}
	return strings.TrimSpace(s[:right])
}

type LogEntry struct {
	QueryID  string // f9464441-7023-4df5-89e5-8d16ea6aa2dd
	Severity string // "Debug", "Information", "Trace"
	Name     string // "MemoryTracker", "executeQuery"
	Message  string // "Peak memory usage (for query): 0.00 B."
	ThreadID uint64 // 591781
}

func (e LogEntry) Level() zapcore.Level {
	switch e.Severity {
	case "Debug", "Trace":
		return zapcore.DebugLevel
	case "Information":
		return zapcore.InfoLevel
	case "Warning":
		return zapcore.WarnLevel
	case "Error":
		return zapcore.ErrorLevel
	case "Fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.DebugLevel
	}
}

func parseLog(s string) LogEntry {
	s = strings.TrimSpace(s)
	tid, _ := strconv.ParseUint(cut(s, "[", "]"), 10, 64)
	var textStart int
	if idx := strings.Index(s, "}"); idx > 0 {
		textStart = strings.Index(s[idx:], ":") + idx + 1
	}
	if textStart-1 > len(s) {
		textStart = 0
	}
	return LogEntry{
		QueryID:  cut(s, "{", "}"),
		Severity: cut(s, "<", ">"),
		Name:     cut(s, ">", ":"),
		Message:  strings.TrimSpace(s[textStart:]),
		ThreadID: tid,
	}
}

// logProxy returns io.Writer that can be used as mongo log output.
//
// The io.Writer will parse json logs and write them to provided logger.
// Call context.CancelFunc on mongo exit.
func logProxy(lg *zap.Logger, f func(info logInfo)) io.Writer {
	r, w := io.Pipe()

	s := bufio.NewScanner(r)

	go func() {
		for s.Scan() {
			e := parseLog(s.Text())

			if ce := lg.Check(e.Level(), e.Message); ce != nil {
				var fields []zap.Field
				if e.QueryID != "" {
					fields = append(fields, zap.String("qid", e.QueryID))
				}
				if e.ThreadID != 0 {
					// Using "pid" to be consistent with ClickHouse log, e.g.:
					// "Will watch for the process with pid 260134"
					fields = append(fields, zap.Uint64("pid", e.ThreadID))
				}
				if e.Name != "" {
					fields = append(fields, zap.String("name", e.Name))
				}
				ce.Write(fields...)
			}

			if strings.Contains(e.Message, "Ready for connections") {
				f(logInfo{Ready: true})
			}
			if !strings.Contains(e.Message, "Listening for") {
				continue
			}

			elems := strings.Split(e.Message, " ")
			f(logInfo{Addr: elems[len(elems)-1]})
		}
	}()

	return w
}
