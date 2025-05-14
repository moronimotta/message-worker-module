package message

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Worker(conn *amqp.Connection, queueName, exchangeName string, handler func(amqp.Delivery)) {
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name of the exchange
		"fanout",     // type of the exchange (e.g., direct, fanout, topic, headers)
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("failed to declare an exchange: %v", err)
	}

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

	// start consuming messages from the queue
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
		log.Printf("Received a message: %s", msg.Body)
		handler(msg)
	}
}
