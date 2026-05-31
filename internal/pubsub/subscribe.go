package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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
				msgAck := handler(v)
				switch msgAck {
				case NackRequeue:
					// fmt.Println("Message is Nack and requeued")
					d.Nack(false, true)
				case NackDiscard:
					// fmt.Println("Message is Nack and discarded")
					d.Nack(false, false)
				default:
					// fmt.Println("Message is Ack")
					d.Ack(false)
				}
			}
		}(msg)
	}

	return nil
}

func decodeGob[T any](data []byte) (T, error) {
	var gl T
	var buf bytes.Buffer

	buf.Write(data)
	dec := gob.NewDecoder(&buf)
	err := dec.Decode(&gl)
	if err != nil {
		return gl, err
	}

	return gl, nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
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
			v, err := decodeGob[T](d.Body)
			if err != nil {
				fmt.Printf("Failed to parse message due to %v\n", err)
			} else {
				fmt.Printf("Subscribing message of value %v\n", v)
				msgAck := handler(v)
				switch msgAck {
				case NackRequeue:
					// fmt.Println("Message is Nack and requeued")
					d.Nack(false, true)
				case NackDiscard:
					// fmt.Println("Message is Nack and discarded")
					d.Nack(false, false)
				default:
					// fmt.Println("Message is Ack")
					d.Ack(false)
				}
			}
		}(msg)
	}

	return nil
}
