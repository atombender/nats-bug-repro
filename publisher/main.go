package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://user:user@127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1000))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "my-stream",
		Retention: jetstream.LimitsPolicy,
		MaxAge:    1 * time.Hour,
		Subjects:  []string{"my-subject.>"},
		Replicas:  0,
	})
	if err != nil {
		panic(err)
	}

	count := 0
	for {
		time.Sleep(time.Duration(500+rand.Intn(2)) * time.Millisecond)

		subject := "my-subject.hello"

		var data []byte
		if time.Now().Unix()%10 == 0 {
			data = []byte(strings.Repeat("beuf!", 1000))
		} else {
			data = []byte(fmt.Sprintf("count=%d", count))
		}
		count++

		fmt.Printf("Publish %s [%d]: %d bytes\n", subject, count, len(data))

		if _, err := js.PublishAsync(subject, data); err != nil {
			panic(err)
		}
	}
}
