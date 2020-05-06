package rabbitmq

type AlertHandler func(title, msg string)

func (client *MqClient) SetAlertHandler(alertHandler AlertHandler) {
	client.AlertHandler = alertHandler
}

func (client *MqClient) alert(title, msg string) {
	if client.AlertHandler != nil {
		client.AlertHandler(title, msg)
	}
}
