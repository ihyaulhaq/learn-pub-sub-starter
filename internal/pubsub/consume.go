package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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

			// handle messages based on the ack type
			switch handler(payload) {
			case Ack:
				log.Println("Ack: message processed successfully")
				msg.Ack(false)
			case NackRequeue:
				log.Println("NackRequeue: message requeued")
				msg.Nack(false, true)
			case NackDiscard:
				log.Println("NackDiscard: message discarded")
				msg.Nack(false, false)
			}
		}
	}()
	return nil
}
