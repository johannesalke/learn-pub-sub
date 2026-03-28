package main

import (
	"fmt"

	"github.com/johannesalke/learn-pub-sub/internal/gamelogic"
	"github.com/johannesalke/learn-pub-sub/internal/pubsub"
	"github.com/johannesalke/learn-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	//"os/signal"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	//fmt.Println("Starting Peril server...")
	con, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Err: %s", err)
	}
	defer con.Close()
	fmt.Print("You are now con-nec-ted.\n")

	chan1, err := con.Channel()
	handleErr(err)
	//err = pubsub.PublishJSON(chan1, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	//handleErr(err)

	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", true)
	handleErr(err)

	err = pubsub.SubscribeGob(con, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug+".*", true, handlerLogs)

	gamelogic.PrintServerHelp()

	for true {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "pause" {
			fmt.Print("Sending a pause message\n")
			err = pubsub.PublishJSON(chan1, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			handleErr(err)
			continue
		}
		if input[0] == "resume" {
			fmt.Print("Sending a resume message\n")
			err = pubsub.PublishJSON(chan1, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			handleErr(err)
			continue
		}
		if input[0] == "quit" {
			fmt.Print("Exiting...\n")
			break
		}
		fmt.Print("Unknown command\n")
	}

	//wait for ctrl+c?
	/*signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")
	*/
}

func handleErr(err error) {
	if err != nil {
		fmt.Printf("Error: %s", err)
		os.Exit(1)
	}
}

func handlerLogs(log routing.GameLog) pubsub.AckType {
	defer fmt.Print("\n> ")
	err := gamelogic.WriteLog(log)
	if err != nil {
		return pubsub.NackDiscard
	}
	return pubsub.AckRecieved
}

/*func handlerLogs() func(gamelog routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.AckRecieved
	}
}*/
