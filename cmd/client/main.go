package main

import (
	"fmt"
	"github.com/johannesalke/learn-pub-sub/internal/gamelogic"
	"github.com/johannesalke/learn-pub-sub/internal/pubsub"
	"github.com/johannesalke/learn-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
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

	username, err := gamelogic.ClientWelcome()
	handleErr(err)

	pausename := strings.Join([]string{routing.PauseKey, username}, ".")
	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, pausename, routing.PauseKey, false)
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
		os.Exit(1)
	}
}
