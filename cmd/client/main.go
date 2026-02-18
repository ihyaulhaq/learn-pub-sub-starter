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
	fmt.Println("Starting Peril client...")

	const url = "amqp://guest:guest@localhost:5672/"

	// make connection to rabbitMq
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("connection to rabbitmq success")

	// get username from input
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf(err.Error())
	}

	// make queue and bind it
	queueName := routing.PauseKey + "." + userName
	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)

	if err != nil {
		log.Fatalf("declare and bind Failed: %v", err)
	}
	fmt.Printf("queue declared and bind : %s\n", queue.Name)

	gameState := gamelogic.NewGameState(userName)

	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}

		command := input[0]
		switch command {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

		case "move":
			armyMove, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Army %v : %v been move to %v\n", armyMove.Units[0].ID, armyMove.Units[0].Rank, armyMove.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			fmt.Println("Shutting down...")
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}

	}
}
