package main

import (
	"fmt"

	"os"
	"os/signal"

	"github.com/johannesalke/learn-pub-sub/internal/pubsub"
	"github.com/johannesalke/learn-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
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
	err = pubsub.PublishJSON(chan1, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	handleErr(err)

	//wait for ctrl+c?
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")

}

func handleErr(err error) {
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
}
