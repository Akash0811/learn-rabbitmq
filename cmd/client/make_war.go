package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(gs *gamelogic.GameState, channel *amqp.Channel, messageLog string) error {
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     messageLog,
		Username:    gs.Player.Username,
	}
	err := pubsub.PublishGob(
		channel,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.Player.Username),
		gl,
	)
	return err
}

func handlerMakeWar(gs *gamelogic.GameState, channel *amqp.Channel) func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(msg)
		var messageLog string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved, gamelogic.WarOutcomeNoUnits:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			messageLog = fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(gs, channel, messageLog)
			if err != nil {
				fmt.Printf("Could not publish gamelog due to %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			messageLog := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(gs, channel, messageLog)
			if err != nil {
				fmt.Printf("Could not publish gamelog due to %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("Could not Handle Make War message")
			return pubsub.NackDiscard
		}
	}
}
