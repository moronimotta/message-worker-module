package message

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Listen establishes a connection to RabbitMQ using the provided Consumer configuration,
// creates a channel (session), declares a topic exchange, a queue, and binds the queue to the
// exchange. It then starts consuming messages
// from the queue and processes each message using the provided handler function.
//
// The function will log fatal errors if any step in the setup process fails.
//
// Parameters:
//   - consumerInput: Consumer configuration containing connection URL, topic (exchange) name,
//     queue name, and event name (routing key).
//   - handler: A function that processes each received amqp.Delivery message.
func Listen(consumerInput Consumer, handler func(amqp.Delivery)) {

	conn, err := amqp.Dial(consumerInput.ConnectionURL)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	err = ch.ExchangeDeclare(
		consumerInput.TopicName, // name of the exchange
		"fanout",                // type of the exchange (e.g., direct, fanout, topic, headers)
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		log.Fatalf("failed to declare an exchange: %v", err)
	}

	q, err := ch.QueueDeclare(
		consumerInput.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare a queue: %v", err)
	}

	// Bind the queue (which the consumer will listen to) to the topic, using an event name as key
	err = ch.QueueBind(
		q.Name,
		"",
		consumerInput.TopicName,
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

	log.Printf(" [*] Waiting for messages in %s. To exit press CTRL+C", q.Name)

	// process each message using the handler function
	for msg := range msgs {
		log.Printf("Received a message: %s", msg.Body)
		handler(msg)
	}
}
