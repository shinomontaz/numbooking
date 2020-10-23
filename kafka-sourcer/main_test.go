package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"numbooking/kafka-sourcer/config"

	"github.com/segmentio/kafka-go"
)

var env *config.Env
var errors chan error

func init() {
	env = config.NewEnv("./config")
	env.InitKafka()
	env.InitLog()

	errors = make(chan error, 1000)
}

func TestSend(t *testing.T) {
	chMessages := make(chan []byte, 1000)
	chOk := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)
	go getCreatedNums(chMessages, chOk)
	go getDeletedNums(chMessages, chOk)

	go func() {
		for err := range errors {
			fmt.Println(err)
		}
	}()

	go func() {
		<-chOk
		wg.Done()
	}()

	go func() {
		for mess := range chMessages { // it is safe to do that due to Writer has internal bacth queue
			err := env.Kafka.WriteMessages(
				context.Background(),
				kafka.Message{Value: mess},
			)
			if err != nil {
				errors <- err
			}
		}
	}()

	wg.Wait()
	close(chOk)
	close(chMessages)
}

func getCreatedNums(chMessages chan<- []byte, chOk chan<- struct{}) {
	chOk <- struct{}{}
}

func getDeletedNums(chMessages chan<- []byte, chOk chan<- struct{}) {
	chOk <- struct{}{}
}
