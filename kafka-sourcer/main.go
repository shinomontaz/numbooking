package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"numbooking/kafka-sourcer/config"

	"github.com/segmentio/kafka-go"

	types "github.com/shinomontaz/numbooking_types"
)

var env *config.Env
var chErrors chan error

func init() {
	env = config.NewEnv("./config")
	env.InitKafka()
	env.InitLog()
	//	env.InitDb()

	chErrors = make(chan error, 1000)

	rand.Seed(time.Now().UnixNano())
}

func main() {

	go func() {
		for err := range chErrors {
			fmt.Println("Error", err)
		}
	}()

	chMessages := make(chan []byte, 1000)
	chOk := make(chan struct{})

	// стартуем

	// читаем из БД по алгоритмам Романа
	// кладем в кафку
	// завершаемся

	var wg, wg2 sync.WaitGroup
	wg.Add(2)
	go getCreatedNums(chMessages, chOk)
	go getDeletedNums(chMessages, chOk)

	go func() {
		for range chOk {
			fmt.Println("Done")
			wg.Done()
		}
	}()

	wg2.Add(1)
	go func() {
		for mess := range chMessages { // it is safe to do that due to Writer has internal bacth queue
			fmt.Println(string(mess))
			err := env.Kafka.WriteMessages(
				context.Background(),
				kafka.Message{Value: mess},
			)
			if err != nil {
				chErrors <- err
			}
		}
		wg2.Done()
	}()

	wg.Wait()

	close(chOk)
	close(chMessages)

	wg2.Wait()

	env.Kafka.Close()
}

func getCreatedNums(chMessages chan<- []byte, chOk chan<- struct{}) {
	defer func() { chOk <- struct{}{} }()
	// 	sql := `SELECT fiRec_hist_rec as fidid_serial
	// 	FROM rec_history
	//    WHERE firec_hist_tab = (
	// 		 SELECT fitable_id FROM tables_tq
	// 		  WHERE fstable_name = 'did_numbers') -- по таблице DID
	// 	 AND farec_hist_dttm > %d -- Старше сегодняшнего дня
	// 	 AND firec_hist_func = 1 -- Создание
	//   `

	// 	sql = fmt.Sprintf(sql, 1)
}

func getDeletedNums(chMessages chan<- []byte, chOk chan<- struct{}) {
	defer func() { chOk <- struct{}{} }()

	// 	sql := `SELECT fiRec_hist_rec as fidid_serial
	// 	FROM rec_history
	//    WHERE firec_hist_tab = (
	// 		 SELECT fitable_id FROM tables_tq
	// 		  WHERE fstable_name = 'did_numbers') -- по таблице DID
	// 	 AND farec_hist_dttm > %d -- Старше сегодняшнего дня
	// 	 AND firec_hist_func = 1 -- Создание
	//   `
	//   sql = fmt.Sprintf(sql)

	var rndID int64
	var rndDelFlag int
	for i := 0; i < 100; i++ {
		rndID = rand.Int63()
		rndDelFlag = rand.Intn(2)
		var delFlag bool
		if rndDelFlag > 0 {
			delFlag = true
		}

		num := types.DidNumber{
			ID:        rndID,
			IsDeleted: delFlag,
		}

		jsonNum, err := json.Marshal(num)
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		chMessages <- jsonNum
	}
}
