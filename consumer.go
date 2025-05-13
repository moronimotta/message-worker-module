package message

import (
	"log"

	"github.com/streadway/amqp"
)

func Worker(conn *amqp.Connection, queueName, exchangeName string, handler func(amqp.Delivery)) {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare a queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind a queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to register a consumer: %v", err)
	}

	log.Printf(" [*] Waiting for messages in %s. To exit press CTRL+C", queueName)

	// process each message using the handler function
	for msg := range msgs {
		handler(msg)
	}
}
