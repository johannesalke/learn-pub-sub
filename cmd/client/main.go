package main

import (
	"fmt"
	"github.com/johannesalke/learn-pub-sub/internal/gamelogic"
	"github.com/johannesalke/learn-pub-sub/internal/pubsub"
	"github.com/johannesalke/learn-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	//"os/signal"
	//"slices"
	"strings"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril client...")
	con, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Err: %s", err)
	}
	defer con.Close()
	fmt.Print("You are now con-nec-ted.\n")
	publishCh, err := con.Channel()
	handleErr(err)

	username, err := gamelogic.ClientWelcome()
	handleErr(err)

	pausename := strings.Join([]string{routing.PauseKey, username}, ".")
	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, pausename, routing.PauseKey, false)
	handleErr(err)

	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON( //|Subscribe Pause
		con, routing.ExchangePerilDirect, pausename,
		routing.PauseKey, false, handlerPause(gamestate),
	)
	handleErr(err)

	move_key := routing.ArmyMovesPrefix + "." + gamestate.GetUsername()
	err = pubsub.SubscribeJSON( //|Subscribe Move
		con, routing.ExchangePerilTopic, move_key,
		"army_moves.*", false, handlerMove(gamestate, publishCh),
	)
	handleErr(err)

	err = pubsub.SubscribeJSON( //Checks war messages across players
		con, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*", true, handlerWar(gamestate),
	)
	handleErr(err)

	for true {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Print("that is not a command. Please try again\n")
			continue
		}
		if input[0] == "spawn" {
			if len(input) != 3 {
				fmt.Print("You need exactly 3 arguments")
				continue
			}
			//if slices.Contains([]string{"infantry","cavalry","artillery"},input[1])
			err := gamestate.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Command error:%s", err)
				continue
			}
			continue
		}
		if input[0] == "move" {
			move, err := gamestate.CommandMove(input)
			if err != nil {
				fmt.Printf("Command error:%s", err)
				continue
			}
			pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, move_key, move)
			//fmt.Printf("Move successful: %s", move)
			continue

		}

		if input[0] == "status" {
			gamestate.CommandStatus()
			continue
		}
		if input[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}
		if input[0] == "spam" {
			fmt.Print("Spamming not allowed yet!\n")
			continue
		}
		if input[0] == "quit" {
			gamelogic.PrintQuit()
			break
		} else {
			fmt.Print("unknown command. type 'help' for an overview of viable commands.")
		}

	}

}

func handleErr(err error) {
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {

	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("\n> ")
		gs.HandlePause(ps)
		return pubsub.AckRecieved
	}

}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {

	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("\n> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.AckRecieved
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.AckRecieved

		case gamelogic.MoveOutcomeMakeWar:

			err := pubsub.PublishJSON(
				ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+move.Player.Username, //gs.GetUsername(),
				gamelogic.RecognitionOfWar{Attacker: move.Player, Defender: gs.GetPlayerSnap()},
			)
			if err != nil {
				fmt.Printf("error:%s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.AckRecieved

		default:
			return pubsub.NackDiscard

		}

	}

}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {

	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, _, _ := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.AckRecieved
		case gamelogic.WarOutcomeYouWon:
			return pubsub.AckRecieved
		case gamelogic.WarOutcomeDraw:
			return pubsub.AckRecieved
		default:
			fmt.Print("Error: War handler could not parse outcome.")
			return pubsub.NackDiscard
		}
	}

}
