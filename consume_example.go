package main

import (
	"log"

	"github.com/zhuxuyang/amqp_go/resource"
)

// 业务逻辑
func myTestConsume(body []byte) (requeue bool, alert bool, err error) {
	log.Println("myTestConsume " + string(body))
	return false, true, nil
}

func InitConsume() {
	resource.MqClient.RegisterConsume("mytest", "", false, 2, myTestConsume)
}
