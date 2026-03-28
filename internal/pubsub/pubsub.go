package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	valJson, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: valJson})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	isDurable bool, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Error creating channel in DeclareAndBind: %s", err)
	}

	//table := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}
	//table["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Error creating queue in DeclareAndBind: %s", err)
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Error binding queue to exchange in DeclareAndBind: %s", err)
	}
	return ch, queue, nil

}

type AckType int

const (
	AckRecieved AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	isDurable bool, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, isDurable)
	if err != nil {
		return fmt.Errorf("Error durcing DeclareAndBind in SubscribeJSON:%s", err)
	}
	ch.Qos(10, 0, false)
	derliveryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Error durcing ch.Consume in SubscribeJSON:%s", err)
	}

	go handleDeliveries(derliveryCh, handler)

	return nil

}

func handleDeliveries[T any](deliveryCh <-chan amqp.Delivery, handler func(T) AckType) {
	for delivery := range deliveryCh {
		var message T
		if delivery.ContentType == "application/json" {
			err := json.Unmarshal(delivery.Body, &message)
			if err != nil {
				fmt.Printf("Error unmarshalling delivery json: %s", err)

			}
		} else if delivery.ContentType == "application/gob" {
			buf := bytes.NewBuffer(delivery.Body)

			decoder := gob.NewDecoder(buf)
			err := decoder.Decode(&message)
			if err != nil {
				fmt.Printf("Error unmarshalling delivery gob: %s", err)
			}

		}

		switch handler(message) {
		case AckRecieved:
			delivery.Ack(false)
			fmt.Print("Delivery Action: Ack\n")
			continue
		case NackRequeue:
			delivery.Nack(false, true)
			fmt.Print("Delivery Action: NackReque\n")
			continue
		case NackDiscard:
			delivery.Nack(false, false)
			fmt.Print("Delivery Action: NackDiscard\n")
			continue
		default:
			delivery.Ack(false)
		}

	}
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(val); err != nil {
		return err
	}
	err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: buf.Bytes()})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	isDurable bool,
	handler func(T) AckType,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, isDurable)
	if err != nil {
		return fmt.Errorf("Error durcing DeclareAndBind in SubscribeGob:%s", err)
	}
	ch.Qos(10, 0, false)
	derliveryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Error durcing ch.Consume in SubscribeJSON:%s", err)
	}
	go handleDeliveries(derliveryCh, handler)
	return nil
}
