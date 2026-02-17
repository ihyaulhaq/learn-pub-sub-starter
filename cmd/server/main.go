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

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}
		command := input[0]
		switch command {
		case "pause":
			fmt.Printf("Sending pause messages")
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
			fmt.Printf("Sending resume messages")
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
	//
	// // wait for ctrl+c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	//
	// fmt.Println("signal receive, shutdown server")

}
