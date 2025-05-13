package message

import (
	"log"

	"github.com/streadway/amqp"
)

func SendMessage(conn *amqp.Connection, message []byte, exchangeName, routingKey string) {

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	defer ch.Close()

	body := message
	err = ch.Publish(
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Fatalf("failed to publish a message: %v", err)
	}

	log.Printf("message sent: %v", body)

}
