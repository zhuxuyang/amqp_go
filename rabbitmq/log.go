package rabbitmq

import (
	"fmt"
)

const (
	LoggerLevelError = 3
	LoggerLevelInfo  = 6
)

type LogHandler func(level int, logMsg string)

func (client *MqClient) SetLogHandler(logHandler LogHandler) {
	client.LogHandler = logHandler
}

func (client *MqClient) LogInfo(LogMsg string) {
	if client.LogHandler != nil {
		client.LogHandler(LoggerLevelInfo, LogMsg)
	}
}

func (client *MqClient) LogInfof(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if client.LogHandler != nil {
		client.LogHandler(LoggerLevelInfo, msg)
	}
}

func (client *MqClient) LogError(LogMsg string) {
	if client.LogHandler != nil {
		client.LogHandler(LoggerLevelError, LogMsg)
	}
}

func (client *MqClient) LogErrorf(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if client.LogHandler != nil {
		client.LogHandler(LoggerLevelError, msg)
	}
}
