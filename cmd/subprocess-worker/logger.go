package main

import (
	"os"

	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
)

func MakeLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stdout"}
	logger, err := cfg.Build()
	if err != nil {
		// Fall back to stderr if logger cannot be initialized.
		_, _ = os.Stderr.WriteString("failed to initialize logger\n")
		os.Exit(1)
	}
	return logger
}

// zapSDKLogger adapts a *zap.Logger to the Temporal SDK log.Logger interface.
type zapSDKLogger struct{ *zap.Logger }

func newZapSDKLogger(l *zap.Logger) log.Logger { return &zapSDKLogger{Logger: l} }

func (z *zapSDKLogger) Debug(msg string, keyvals ...any) {
	z.Logger.Debug(msg, keyvalsToZapFields(keyvals)...)
}

func (z *zapSDKLogger) Info(msg string, keyvals ...any) {
	z.Logger.Info(msg, keyvalsToZapFields(keyvals)...)
}

func (z *zapSDKLogger) Warn(msg string, keyvals ...any) {
	z.Logger.Warn(msg, keyvalsToZapFields(keyvals)...)
}

func (z *zapSDKLogger) Error(msg string, keyvals ...any) {
	z.Logger.Error(msg, keyvalsToZapFields(keyvals)...)
}

func keyvalsToZapFields(keyvals []any) []zap.Field {
	fields := make([]zap.Field, 0, (len(keyvals)+1)/2)
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key, ok := keyvals[i].(string)
			if !ok {
				key = "key"
			}
			fields = append(fields, zap.Any(key, keyvals[i+1]))
		} else {
			fields = append(fields, zap.Any("value", keyvals[i]))
		}
	}
	return fields
}
