package controllers

import (
	"time"

	"github.com/brkelkar/common_utils/logger"
	"go.uber.org/zap"
)

var (
	serviceName = "sync_util_uplaod"
	//RequestType used to report type of requiest
	RequestType string
)

//LogResponceEvent responce logger
func LogResponceEvent(responceType string, rVal int, responceLen int, timeNow time.Time) {
	duration := int64(time.Since(timeNow) / 1000000)
	logger.Info("Responce", zap.String("service", serviceName),
		zap.String("type", RequestType),
		zap.String("responceType", responceType),
		zap.Int("status_code", rVal),
		zap.Int64("process_time_ms", duration),
		zap.Int("record_count", responceLen))
}
