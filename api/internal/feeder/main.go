package feeder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"

	"numbooking/api/internal/types"

	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
)

type Service struct {
	r    kafka.Reader
	errs chan<- error
	out  chan<- kafka.Message
	db   *sqlx.DB
}

func New(urls []string, topic string, errs chan error, db *sqlx.DB) *Service {
	return &Service{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   urls,
			Topic:     topic,
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
		errs: errs,
		db:   db,
	}
}

func (s *Service) Run() {
	defer s.r.Close()
	for {
		m, err := s.r.ReadMessage(context.Background())
		if !s.HandleError(err) {
			return
		}
		go s.Process(m)
		//		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

func (s *Service) Process(m kafka.Message) {
	sql := ""
	var n types.Number
	switch string(m.Key) {
	case "POST":
		err := json.Unmarshal(m.Value, &n)
		if !s.HandleError(err) {
			return
		}
		sql = fmt.Sprintf("INSERT INTO number (number, beauty, created_at ) VALUES (%d, $d, %d)", n.Number, n.Beauty, n.Created_at)
	case "PUT":
		err := json.Unmarshal(m.Value, &n)
		if !s.HandleError(err) {
			return
		}
		sql = fmt.Sprintf("INSERT INTO number (number, beauty ) VALUES (%d, %d)", n.Number, n.Beauty)
	case "DELETE":
		err := json.Unmarshal(m.Value, &n)
		if !s.HandleError(err) {
			return
		}
		sql = fmt.Sprintf("UPDATE number SET is_deleted = TRUE WHERE number = %d", n.Number)
	default:
		s.errs <- errors.New(fmt.Sprintf("unsupported key %s - %s", string(m.Key), string(m.Value)))
		return
	}

	_, err := s.db.Query(sql)
	if !s.HandleError(err) {
		return
	}
}

func (s *Service) HandleError(err error) bool {
	if err != nil {
		_, filename, lineno, _ := runtime.Caller(2)
		s.errs <- errors.New(fmt.Sprintf("Error: %v (%v:%v)\n", err, filename, lineno))
		return false
	}
	return true
}
