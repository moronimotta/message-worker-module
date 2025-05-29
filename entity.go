package message

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Event struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Event) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		return err
	}
	return nil
}

type Consumer struct {
	ConnectionURL string
	QueueName     string
	EventName     string
	TopicName     string
}

type Publisher struct {
	ConnectionURL string
	Connection    *amqp.Connection
	TopicName     string
}
