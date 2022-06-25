package utils

import (
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func SetupLogging() {
	formatter := &log.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    false,
	}
	log.SetFormatter(formatter)
	level := os.Getenv("DATAOS_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "error":
		log.SetLevel(log.InfoLevel)
	case "debug":
		{
			formatter := &log.TextFormatter{
				DisableTimestamp: false,
				FullTimestamp:    true,
				TimestampFormat:  time.StampMicro,
			}
			log.SetFormatter(formatter)
			log.SetLevel(log.DebugLevel)
		}
	case "trace":
		{
			formatter := &log.TextFormatter{
				DisableTimestamp: false,
				FullTimestamp:    true,
				TimestampFormat:  time.StampMicro,
			}
			log.SetFormatter(formatter)
			log.SetLevel(log.TraceLevel)
		}

	default:
		log.SetLevel(log.InfoLevel)
	}
	log.Trace("setupLogging()...exit")
}
