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

/*
2022.01.19 16:58:24.256479 [ 591781 ] {} <Trace> ContextAccess (default): Settings: readonly=0, allow_ddl=true, allow_introspection_functions=false
2022.01.19 16:58:24.256484 [ 591781 ] {} <Trace> ContextAccess (default): List of all grants: GRANT SHOW, SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, KILL QUERY, MOVE PARTITION BETWEEN SHARDS, SYSTEM, dictGet, INTROSPECTION, SOURCES ON *.*
2022.01.19 16:58:24.256488 [ 591781 ] {} <Trace> ContextAccess (default): List of all grants including implicit: GRANT SHOW, SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, KILL QUERY, MOVE PARTITION BETWEEN SHARDS, SYSTEM, dictGet, INTROSPECTION, SOURCES ON *.*
2022.01.19 16:58:24.256559 [ 591781 ] {} <Debug> TCP-Session: b75e863b-c0c0-4b7b-b75e-863bc0c08b7b Creating query context from session context, user_id: 94309d50-4f52-5250-31bd-74fecac179db, parent context user: default
2022.01.19 16:58:24.256691 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Debug> executeQuery: (from 127.0.0.1:59428) SELECT * FROM system.clusters
2022.01.19 16:58:24.256931 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Trace> ContextAccess (default): Access granted: SELECT(cluster, shard_num, shard_weight, replica_num, host_name, host_address, port, is_local, user, default_database, errors_count, slowdowns_count, estimated_recovery_time) ON system.clusters
2022.01.19 16:58:24.257025 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Trace> InterpreterSelectQuery: FetchColumns -> Complete
2022.01.19 16:58:24.257527 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Information> executeQuery: Read 2 rows, 204.00 B in 0.000814392 sec., 2455 rows/sec., 244.62 KiB/sec.
2022.01.19 16:58:24.257537 [ 591781 ] {f9464441-7023-4df5-89e5-8d16ea6aa2dd} <Debug> MemoryTracker: Peak memory usage (for query): 0.00 B.
*/

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
					fields = append(fields, zap.Uint64("tid", e.ThreadID))
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
