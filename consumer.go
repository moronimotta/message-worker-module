package message

import (
	"log"
	"strings"

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
//   - handler: A function that processes each Event received from the queue and retuns an error.
func Listen(consumerInput Consumer, handler func(event Event) error) {

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
		false,
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
		var event Event
		err := event.Unmarshal(msg.Body)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			// Reject the message, don't requeue malformed messages
			msg.Nack(false, false)
			continue // Skip to next message, don't ack
		}
		err = handler(event)
		if err != nil {
			log.Printf("Handler error: %v", err)
			// Decide whether to requeue or reject based on error type
			if isRetryableError(err) {
				log.Printf("Retryable error, requeuing message: %v", err)
				msg.Nack(false, true) // Requeue for retry
			} else {
				log.Printf("Non-retryable error, rejecting message: %v", err)
				msg.Nack(false, false) // Don't requeue
			}
			continue // Skip to next message, don't ack
		}

		// Acknowledge the message only if no errors occurred
		msg.Ack(false)
	}
}

// isRetryableError determines if an error should trigger a message requeue
func isRetryableError(err error) bool {
	errorMsg := strings.ToLower(err.Error())

	// Retryable errors (temporary issues)
	retryablePatterns := []string{
		"connection refused",
		"timeout",
		"temporary failure",
		"database is locked",
		"too many connections",
		"network unreachable",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errorMsg, pattern) {
			return true
		}
	}

	// Non-retryable errors (permanent issues)
	nonRetryablePatterns := []string{
		"invalid",
		"malformed",
		"not found",
		"unauthorized",
		"forbidden",
		"unhandled event type",
		"failed to unmarshal",
		"event data is not of type",
	}

	for _, pattern := range nonRetryablePatterns {
		if strings.Contains(errorMsg, pattern) {
			return false
		}
	}

	// Default to retryable for unknown errors
	return true
}
