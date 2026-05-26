package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	channel, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveryChannel, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range deliveryChannel {
		go func(d amqp.Delivery) {
			var v T
			err := json.Unmarshal(d.Body, &v)
			if err != nil {
				fmt.Printf("Failed to parse message due to %v\n", err)
			} else {
				fmt.Printf("Subscribing message of value %v\n", v)
				handler(v)
				d.Ack(false)
			}
		}(msg)
	}

	return nil
}
