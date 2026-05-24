package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const connectionString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Println("Client could not connect to RabbitMQ")
		return
	}
	defer conn.Close()

	uname, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Cannot get user name due to %v\n", err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, uname),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		fmt.Printf("Failed to create queue due to %v\n", err)
		return
	}

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Received CTRL+C...")
	fmt.Println("Shutting Down...")
}
