package log

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New will create a new logger based on the level and output format
func New(level, output string) (*zap.Logger, error) {
	atomicLevel := zap.NewAtomicLevel()
	switch strings.ToUpper(level) {
	case "DEBUG":
		atomicLevel.SetLevel(zap.DebugLevel)
	case "INFO":
		atomicLevel.SetLevel(zap.InfoLevel)
	case "WARN":
		atomicLevel.SetLevel(zap.WarnLevel)
	case "ERROR":
		atomicLevel.SetLevel(zap.ErrorLevel)
	default:
		atomicLevel.SetLevel(zap.InfoLevel)
	}
	return newWithLevel(atomicLevel, output)
}

func newWithLevel(level zap.AtomicLevel, output string) (*zap.Logger, error) {
	logConfig := zap.NewProductionConfig()
	logConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logConfig.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	logConfig.EncoderConfig.ConsoleSeparator = " "
	logConfig.Encoding = output
	logConfig.Level = level
	return logConfig.Build()
}
