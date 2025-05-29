package message

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SendMessage checks if a RabbitMQ connection is available in the provided Publisher input,
// or creates a new one if not. It receives a message of type Event, marshals it into bytes,
// and sends it as the body of a message to a specific topic using RabbitMQ.
func SendMessage(publisherInput Publisher, message Event) {
	var (
		conn *amqp.Connection
		ch   *amqp.Channel
		err  error
	)

	// Use existing connection if available
	if publisherInput.Connection != nil {
		conn = publisherInput.Connection
	} else {
		conn, err = amqp.Dial(publisherInput.ConnectionURL)
		if err != nil {
			log.Fatalf("failed to connect to RabbitMQ: %v", err)
		}
		defer conn.Close()
	}

	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	messageBytes, err := message.Marshal()
	if err != nil {
		log.Fatalf("failed to marshal message: %v", err)
	}

	err = ch.Publish(
		publisherInput.TopicName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        messageBytes,
		},
	)
	if err != nil {
		log.Fatalf("failed to publish a message: %v", err)
	}

	log.Printf("message sent: %v", string(messageBytes))
}
