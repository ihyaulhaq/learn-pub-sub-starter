package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	// ensure q exist
	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	// consume the q from rabbitmq
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
		return err
	}

	go func() {
		for msg := range msgs {
			var payload T
			// parse the body to T type
			err := json.Unmarshal(msg.Body, &payload)
			if err != nil {
				msg.Nack(false, false)
				continue
			}
			// handle the data
			handler(payload)
			msg.Ack(false)
		}
	}()
	return nil
}
