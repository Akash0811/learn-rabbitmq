package main

import (
	"fmt"
	"strconv"
	"time"

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

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println("Failed to create channel")
		return
	}

	state := gamelogic.NewGameState(uname)

	go pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, uname),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(state),
	)

	go pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, uname),
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, "*"),
		pubsub.Transient,
		handlerMove(state, channel),
	)

	go pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, "*"),
		pubsub.Durable,
		handlerMakeWar(state, channel),
	)
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
			move, err := state.CommandMove(inputs)
			if err != nil {
				fmt.Printf("Failed to move due to %v\n", err)
			} else {
				err = pubsub.PublishJSON(
					channel,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, uname),
					move,
				)
				if err != nil {
					fmt.Printf("Failed to move due to %v\n", err)
					continue
				}
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
			if len(inputs) != 2 {
				fmt.Println("Expecting a integer argument with spam")
				continue
			}

			num, err := strconv.Atoi(inputs[1])
			if err != nil {
				fmt.Printf("Failed to convert integer due to %v\n", err)
				continue
			}

			for i := 0; i < num; i++ {
				maliciousLog := gamelogic.GetMaliciousLog()
				pubsub.PublishGob(
					channel,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, uname),
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     maliciousLog,
						Username:    uname,
					},
				)
			}
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
