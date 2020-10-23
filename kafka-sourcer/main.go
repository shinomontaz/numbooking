package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"numbooking/kafka-sourcer/config"

	"github.com/segmentio/kafka-go"

	types "github.com/shinomontaz/numbooking_types"
)

var env *config.Env
var errors chan error

func init() {
	env = config.NewEnv("./config")
	env.InitKafka()
	env.InitLog()
	//	env.InitDb()

	errors = make(chan error, 1000)

}

func main() {

	chMessages := make(chan []byte, 1000)
	chOk := make(chan struct{})

	// стартуем

	// читаем из БД по алгоритмам Романа
	// кладем в кафку
	// завершаемся

	var wg sync.WaitGroup
	wg.Add(2)
	go getCreatedNums(chMessages, chOk)
	go getDeletedNums(chMessages, chOk)

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
	sql := `SELECT fiRec_hist_rec as fidid_serial
	FROM rec_history
   WHERE firec_hist_tab = (
		 SELECT fitable_id FROM tables_tq 
		  WHERE fstable_name = 'did_numbers') -- по таблице DID
	 AND farec_hist_dttm > %d -- Старше сегодняшнего дня
	 AND firec_hist_func = 1 -- Создание
  `

	sql = fmt.Sprintf(sql, 1)

	chOk <- struct{}{}
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

	num := types.DidNumber{}

	jsonNum, err := json.Marshal(num)
	if err != nil {
		log.Fatal(err)
	}
	chMessages <- jsonNum

}
