package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		fmt.Printf("Outcome of move is %v\n", moveOutcome)
	}
}
