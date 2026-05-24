package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const connectionString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Println("Server could not connect to RabbitMQ")
		return
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ server")

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to create channel")
		return
	}

	// run the game
	gamelogic.PrintServerHelp()
	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		if inputs[0] == "pause" {
			fmt.Println("Sending Pause message")
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				fmt.Printf("Failed to publish Pause data due to %v\n", err)
			}
			continue
		}
		if inputs[0] == "resume" {
			fmt.Println("Sending Resume message")
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				fmt.Printf("Failed to publish Resume data due to %v\n", err)
			}
			continue
		}
		if inputs[0] == "quit" {
			fmt.Println("Exiting Server...")
			break
		} else {
			fmt.Println("Cannot understand command provided")
			continue
		}
	}
}
