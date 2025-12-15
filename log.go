package relay

import (
	"fmt"
	"log/slog"
	"os"
)

type Logger interface {
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
}

type slogLogger struct {
	l *slog.Logger
}

func defaultLogger() Logger {
	return &slogLogger{l: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))}
}

func (s *slogLogger) Debug(args ...any) { s.l.Debug(sprint(args)) }
func (s *slogLogger) Info(args ...any)  { s.l.Info(sprint(args)) }
func (s *slogLogger) Warn(args ...any)  { s.l.Warn(sprint(args)) }
func (s *slogLogger) Error(args ...any) { s.l.Error(sprint(args)) }

func sprint(args []any) string {
	if len(args) == 1 {
		if s, ok := args[0].(string); ok {
			return s
		}
	}
	return fmt.Sprint(args...)
}
