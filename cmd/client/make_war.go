package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)

func handlerMakeWar(gs *gamelogic.GameState) func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(msg)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved, gamelogic.WarOutcomeNoUnits:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeDraw:
			return pubsub.Ack
		default:
			fmt.Println("Could not Handle Make War message")
			return pubsub.NackDiscard
		}
	}
}
