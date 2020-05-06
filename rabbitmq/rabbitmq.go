package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type MqClient struct {
	Url         string
	MqTaskMap   map[string]*MqTask
	NotifyClose chan *amqp.Error
	mqConn      *amqp.Connection

	LogHandler   LogHandler   // 记录日志的回调
	AlertHandler AlertHandler // 钉钉消息回调

	beforeReconnect beforeReconnect // 可以在重连前处理一些事情，比如重连的时候需要修改url
}

type beforeReconnect func(client *MqClient)

type MqTask struct {
	PublishChannel *amqp.Channel
	ConsumeChannel *amqp.Channel
	QueueName      string

	PublishNotify bool // 发送是否使用 NotifyConfirm 校验
	NotifyConfirm chan amqp.Confirmation
	RetryCount    int // 消息重试机制

	BindKey        string
	AutoAck        bool
	GoroutineCnt   int
	ConsumeHandler ConsumeHandler
}

type ConsumeHandler func(body []byte) (requeue bool, alert bool, err error)

/*
在自己的服务启动的时候初始化
*/
func InitMqClient(url string) *MqClient {
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(fmt.Sprintf("InitMqClient err url %s %v", url, err))
	}

	notifyClose := make(chan *amqp.Error)

	client := &MqClient{
		MqTaskMap:   make(map[string]*MqTask, 0),
		NotifyClose: notifyClose,
		mqConn:      conn,
		Url:         url,
	}
	conn.NotifyClose(client.NotifyClose)
	go client.closeListener()
	return client
}

/*
 在初始化后注册自己服务内需要用到发送的队列
*/
func (client *MqClient) RegisterPublish(queueName string, publishNotify bool, retryCount int) {

	task, h := client.MqTaskMap[queueName]
	if !h {
		task = &MqTask{
			PublishNotify: publishNotify,
			RetryCount:    retryCount, // 消息重试机制
			QueueName:     queueName,
		}
	}
	publishChannel, err := client.mqConn.Channel()
	if err != nil {
		panic(fmt.Sprintf("RegisterPublish %s err %v", queueName, err))
	}

	_, err = publishChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("RegisterPublish %s err %v", queueName, err))
	}
	if publishNotify {
		err = publishChannel.Confirm(false)
		if err != nil {
			panic(fmt.Sprintf("RegisterPublish %s err %v", queueName, err))
		}

		if task.PublishNotify {
			task.NotifyConfirm = publishChannel.NotifyPublish(make(chan amqp.Confirmation))
		}
	}

	task.PublishChannel = publishChannel
	client.MqTaskMap[queueName] = task
}

/*
统一发送封装
*/
func (client *MqClient) Publish(queueName string, msg interface{}) error {
	task, h := client.MqTaskMap[queueName]
	if task == nil || !h {
		return errors.New("rabbitMQ Publish err [" + queueName + "] task is nil")
	}

	message, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	messageID := fmt.Sprintf("%s-%d", queueName, time.Now().UnixNano())

	for i := 0; i < task.RetryCount; i++ {
		err := task.PublishChannel.Publish(
			"",
			queueName,
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/json",
				Body:         message,
				DeliveryMode: amqp.Persistent,
				MessageId:    messageID,
			},
		)

		if err != nil {
			client.LogErrorf("task.PublishChannel.Publish err %v", err)
			if err == amqp.ErrClosed {
				client.reconnect()
			}
			continue
		} else {

			if task.PublishNotify { // 设置了需要发送确认
				timeOut := time.After(5 * time.Second)
				select {
				case confirm := <-task.NotifyConfirm:
					if !confirm.Ack {
						s := fmt.Sprintf("publish confirm error %s %s", messageID, string(message))
						err = errors.New(s)
						client.LogError(s)
						client.alert("confirm.Ack false", s)
						continue
					}
				case <-timeOut:
					s := fmt.Sprintf("publish confirm timeOut %s %s", messageID, string(message))
					err = errors.New(s)
					client.LogError(s)
					client.alert("confirm.Ack timeOut", s)
					continue
				}
			}

			s := fmt.Sprintf("rabbitmq publish ok %s %s", messageID, string(message))
			client.LogInfo(s)
			return nil
		}
	}
	return err
}

// 在自己服务内注册消费者
func (client *MqClient) RegisterConsume(queueName string, bindKey string, autoAck bool, goroutineCnt int, rmqHandler ConsumeHandler) {
	task, h := client.MqTaskMap[queueName]
	if task == nil || !h {
		task = &MqTask{
			BindKey:        queueName,
			AutoAck:        autoAck,
			ConsumeHandler: rmqHandler,
		}
		client.MqTaskMap[queueName] = task
	}

	channel, err := client.mqConn.Channel()
	if err != nil {
		panic(fmt.Sprintf("rabbitmq RegisterConsume err %v", err))
	}

	if bindKey != "" {
		err := channel.QueueBind(
			queueName,
			bindKey,
			"",
			false,
			nil)

		if err != nil {
			panic(err)
		}
	}
	task.ConsumeChannel = channel
	task.GoroutineCnt = goroutineCnt
	task.ConsumeHandler = rmqHandler
	msgs, err := task.ConsumeChannel.Consume(
		queueName,
		queueName,
		autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	for i := 0; i < goroutineCnt; i++ {
		go func() {
			for d := range msgs {
				client.LogInfof("%s do deliveries read body is %s, msgId is %s", queueName, string(d.Body), d.MessageId)
				requeue, alert, err := rmqHandler(d.Body)

				if err != nil {
					client.LogError(fmt.Sprintf("%s consumeHandler failed, msgId:%s err:%v", queueName, d.MessageId, err))
				}

				if alert {
					client.alert("consumeHandler alert", fmt.Sprintf("%s consumeHandler failed, msgId:%s err:%v", queueName, d.MessageId, err))
				}

				if requeue {
					client.LogInfo(fmt.Sprintf("%s consumeHandler Nack, msgId:%s err:%v", queueName, d.MessageId, err))
					_ = d.Nack(false, true)
				} else {
					client.LogInfo(fmt.Sprintf("%s consumeHandler Ack, msgId:%s  ", queueName, d.MessageId))
					_ = d.Ack(false)
				}
			}
		}()
	}
}

// 安全退出
func (client *MqClient) GracefulStop() {
	for key, value := range client.MqTaskMap {
		if value != nil && value.PublishChannel != nil {
			err := value.PublishChannel.Cancel(key, false)
			if err != nil {
				client.LogErrorf("rabbitmq GracefulStop %s PublishChannel failed, err is %s", key, err.Error())
			}
		}

		if value != nil && value.ConsumeChannel != nil {
			err := value.ConsumeChannel.Cancel(key, false)
			if err != nil {
				client.LogErrorf("rabbitmq GracefulStop %s ConsumeChannel failed, err is %s", key, err.Error())
			}
		}
	}
	if client.mqConn != nil {
		_ = client.mqConn.Close()
	}
}

func (client *MqClient) reconnect() {
	client.LogInfo("MqClient reconnect start ...")
	if client.beforeReconnect != nil {
		client.beforeReconnect(client)
	}
	conn, err := amqp.Dial(client.Url)
	if err != nil {
		panic(fmt.Sprintf("InitMqClient err url %s %v", client.Url, err))
	}
	client.mqConn = conn
	client.NotifyClose = make(chan *amqp.Error)
	conn.NotifyClose(client.NotifyClose)

	for key, task := range client.MqTaskMap {
		if task != nil && task.PublishChannel != nil {
			client.RegisterPublish(key, task.PublishNotify, task.RetryCount)
		}

		if task != nil && task.ConsumeChannel != nil {
			client.RegisterConsume(key, task.BindKey, task.AutoAck, task.GoroutineCnt, task.ConsumeHandler)
		}
	}

	client.LogInfo("MqClient reconnect end ...")

}

func (client *MqClient) closeListener() {
	defer func() {
		if r := recover(); r != nil {
			client.LogErrorf("closeListener %v", r)
			client.reconnect()
		}
	}()

	for {
		<-client.NotifyClose
		client.LogInfo("rabbitMq closeListener notifyClose ")
		client.LogInfo("rabbitMq closeListener 开启重连 ")
		client.reconnect()
		time.Sleep(10 * time.Second)
	}
}
