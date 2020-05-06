package resource

import (
	"fmt"
	"log"

	go_logger "github.com/phachon/go-logger"
	"github.com/spf13/viper"
	"github.com/zhuxuyang/amqp_go/rabbitmq"
)

var MqClient *rabbitmq.MqClient

func InitMQ(logger *go_logger.Logger) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s",
		viper.Get("RabbitMQ.user"),
		viper.Get("RabbitMQ.password"),
		viper.Get("RabbitMQ.address"),
		viper.Get("RabbitMQ.port"),
	)

	MqClient = rabbitmq.InitMqClient(url)

	// 日志回调。可以对日志格式重组，不需要就不配置
	MqClient.SetLogHandler(func(level int, logMsg string) {
		if level == rabbitmq.LoggerLevelError {
			logger.Error(logMsg)
		} else {
			logger.Infof(logMsg)
		}
	})

	// 消息回调，不需要就不配置
	MqClient.SetAlertHandler(func(title, msg string) {
		// 自己在项目里发到对应的地方
		log.Println("SetAlertHandler ", title, " ", msg)
	})

	MqClient.RegisterPublish("mytest", true, 3)
}
