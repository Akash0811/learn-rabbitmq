package main

import (
	"fmt"

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

	state := gamelogic.NewGameState(uname)

	// Start REPL Game Loop
	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		// spawn command
		if inputs[0] == "spawn" {
			err = state.CommandSpawn(inputs)
			if err != nil {
				fmt.Printf("Failed due to %v\n", err)
				fmt.Println("Example Usage: spawn europe infantry")
			}
			continue
		}

		// move command
		if inputs[0] == "move" {
			_, err = state.CommandMove(inputs)
			if err != nil {
				fmt.Printf("Failed due to %v\n", err)
				fmt.Println("Example Usage: spawn europe infantry")
			} else {
				fmt.Println("Successful Move!")
			}
			continue
		}

		// status command
		if inputs[0] == "status" {
			state.CommandStatus()
			continue
		}

		// help command
		if inputs[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}

		// spam command
		if inputs[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
			continue
		}

		// quit command
		if inputs[0] == "quit" {
			gamelogic.PrintQuit()
			break
		}

		// others
		fmt.Println("Command is not recognized")
	}

	// wait for ctrl+c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println("Received CTRL+C...")
	// fmt.Println("Shutting Down...")
}
