package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const url = "amqp://guest:guest@localhost:5672/"

	// make connection to rabbitMq
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	//close connection
	defer conn.Close()

	// create rabbit channel
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to make channel to rabbitmq: %v", err)
	}

	fmt.Println("connection to rabbitmq success")

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".#",
		pubsub.SimpleQueueDurable,
		handleLog(),
	)
	if err != nil {
		log.Fatalf("cant subscibe to log queue: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}
		command := input[0]
		switch command {
		case "pause":
			fmt.Println("Sending pause messages")
			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("Could not publish time: %v", err)
			}

		case "resume":
			fmt.Println("Sending resume messages")
			err = pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("Could not publish time: %v", err)
			}

			// handle resume
		case "quit":
			fmt.Println("Shutting down...")
			return
		default:
			fmt.Println("unknown command")
		}

	}
}

func handleLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {

		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack

	}
}
