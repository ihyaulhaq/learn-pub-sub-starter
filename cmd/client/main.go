package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	// "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	// "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf(err.Error())
	}

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

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("signal receive, shutdown server")

}
