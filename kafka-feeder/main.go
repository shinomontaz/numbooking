package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"numbooking/kafka-feeder/config"

	types "github.com/shinomontaz/numbooking_types"

	"github.com/segmentio/kafka-go"
)

var env *config.Env
var chErrors chan error

func init() {
	env = config.NewEnv("./config")
	env.InitKafka()
	env.InitLog()
	env.InitDb()

	chErrors = make(chan error, 1000)

	rand.Seed(time.Now().UnixNano())
}

func main() {
	go func() {
		for err := range chErrors {
			fmt.Println(err)
		}
	}()

	for {
		m, err := env.Kafka.ReadMessage(context.Background())
		if err != nil {
			chErrors <- err
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		go Process(m) // TODO: make a semaphore
	}
	env.Kafka.Close()
}

// type DidNumber struct {
// 	ID           int64
// 	Number       string
// 	IsDeleted    bool
// 	BookingCode  string
// 	ReservedAt   time.Time
// 	ReservedTill time.Time
// 	// selected_at DateTime('UTC'), /* время выбора номера для просмотра */
// 	// selected_till DateTime('UTC'), /* время сокрытия номера без брони из поисковой выдачи */
// 	beauty int64
// 	region int64

// 	Provider int64 // provider ID
// 	Type     int
// }

func Process(m kafka.Message) {
	sql := ""
	var n types.DidNumber
	switch string(m.Key) {
	case "POST":
		err := json.Unmarshal(m.Value, &n)
		if err != nil {
			chErrors <- err
			return
		}
		//		sql = fmt.Sprintf("INSERT INTO number (number, beauty, created_at ) VALUES (%d, $d, %d)", n.Number, n.Beauty, n.Created_at)
	case "PUT":
		err := json.Unmarshal(m.Value, &n)
		if err != nil {
			chErrors <- err
			return
		}
		//		sql = fmt.Sprintf("INSERT INTO number (number, beauty ) VALUES (%d, %d)", n.Number, n.Beauty)
	case "DELETE":
		err := json.Unmarshal(m.Value, &n)
		if err != nil {
			chErrors <- err
			return
		}
		sql = fmt.Sprintf("UPDATE number SET is_deleted = TRUE WHERE number = %d", n.Number)
	default:
		chErrors <- errors.New(fmt.Sprintf("unsupported key %s - %s", string(m.Key), string(m.Value)))
	}

	if sql != "" {
		_, err := env.Db.Query(sql)
		if err != nil {
			chErrors <- err
		}
	}
}

// func handleError(err error) bool {
// 	if err != nil {
// 		_, filename, lineno, _ := runtime.Caller(2)
// 		s.errs <- errors.New(fmt.Sprintf("Error: %v (%v:%v)\n", err, filename, lineno))
// 		return false
// 	}
// 	return true
// }
