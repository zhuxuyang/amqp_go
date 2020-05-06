package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/phachon/go-logger"
	"github.com/spf13/viper"
	"github.com/zhuxuyang/amqp_go/resource"
)

func main() {
	initConfig()

	logger := go_logger.NewLogger()
	// 包里要记日志，将项目自己的日志放进去
	resource.InitMQ(logger)

	InitConsume()
	log.Println(resource.MqClient.Publish("mytest", struct {
		Name string
	}{time.Now().Format("2006-01-02 15:04:05")}))

	time.Sleep(1 * time.Hour)
}

func initConfig() {
	var (
		configFile = flag.String("conf", "./rabbitmq/config/config.yaml", "path of config file")
	)
	if *configFile == "" {
		flag.Usage()
	}
	viper.SetConfigFile(*configFile)
	err := viper.ReadInConfig()
	if err != nil {
		errStr := fmt.Sprintf("viper read config is failed, err is %v configFile is %v ", err, configFile)
		panic(errStr)
	}
}
