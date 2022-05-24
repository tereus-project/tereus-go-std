package logging

import (
	"strings"

	"github.com/onrik/logrus/filename"
	logrus_sentry "github.com/onrik/logrus/sentry"
	"github.com/sirupsen/logrus"
)

type LogConfig struct {
	Format       string
	LogLevel     string
	SentryDSN    string
	SentryProxy  string
	ReportCaller bool
	ShowFilename bool
	Env          string
}

type SentryConfig struct {
	DSN   string
	Proxy string
}

type Log struct {
	Level  string
	Format string
}

var SentryHook *logrus_sentry.Hook

func SetupLog(config LogConfig) (*logrus_sentry.Hook, error) {

	switch strings.ToLower(config.Format) {
	case "raw":
		logrus.SetFormatter(&logrus.TextFormatter{})
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{})
	default:
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	switch strings.ToLower(config.LogLevel) {
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	if config.ReportCaller {
		logrus.SetReportCaller(true)
	}

	if config.ShowFilename {
		// Cleaner SetReportCaller
		filenameHook := filename.NewHook()
		filenameHook.Field = "file"
		logrus.AddHook(filenameHook)
	}

	if config.SentryDSN != "" {
		SentryHook, err := logrus_sentry.NewHook(logrus_sentry.Options{
			Dsn:              config.SentryDSN,
			HTTPProxy:        config.SentryProxy,
			HTTPSProxy:       config.SentryProxy,
			Release:          "todo",
			AttachStacktrace: true,
			Environment:      getEnv(config.Env),
		},
			// Report to sentry when Error or above
			logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel)
		if err != nil {
			return nil, err
		}
		logrus.AddHook(SentryHook)

		return SentryHook, err
	}

	return nil, nil
}

func RecoverAndLogPanic() {
	if err := recover(); err != nil {
		// This will also report the panic to Sentry (if enabled)
		logrus.Panic(err)
	}
}

func getEnv(env string) string {
	if env == "" {
		return "unknown"
	}

	return env
}
