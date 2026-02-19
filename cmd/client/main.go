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
	gameState := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+userName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("can't Subscribe to queue %v: %v", routing.PauseKey+"."+userName, err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+userName,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState),
	)
	if err != nil {
		log.Fatalf("can't subscribe to army moves: %v", err)
	}

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
			// get army move
			armyMove, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			// open channel to rabbitmq
			ch, err := conn.Channel()
			if err != nil {
				fmt.Println("failed to open channel:", err)
				return
			}
			defer ch.Close()

			//publisd the move
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+userName,
				armyMove,
			)
			if err != nil {
				fmt.Println("failed to publish move:", err)
				continue
			}

			fmt.Println("move published successfully")
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe,
			gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}
