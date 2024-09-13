package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://user:user@127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	consumer, err := js.CreateConsumer(ctx, "my-stream", jetstream.ConsumerConfig{
		Name:               "my-consumer",
		Durable:            "my-consumer",
		AckPolicy:          jetstream.AckExplicitPolicy,
		AckWait:            5 * time.Second,
		DeliverPolicy:      jetstream.DeliverLastPolicy,
		MaxRequestMaxBytes: 1000,
		FilterSubjects:     []string{"my-subject.*"},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Start consume\n")
	_, err = consumer.Consume(
		func(msg jetstream.Msg) {
			m, err := msg.Metadata()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Received %d (%d bytes)\n", m.Sequence.Stream, len(msg.Data()))
			_ = msg.Ack()
		},
		jetstream.PullExpiry(5*time.Minute),
		jetstream.PullMaxBytes(1000),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			fmt.Printf("Consumer error: %s\n", err)
		}))
	if err != nil {
		panic(err)
	}

	var ch chan struct{}
	<-ch
}
