package api

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/jmoiron/sqlx"

	"github.com/shinomontaz/numbooking/internal/types"
)

type Service struct {
	db   *sqlx.DB
	errs chan<- error
}

func New(db *sqlx.DB, errs chan<- error) *Service {
	s := &Service{
		db:   db,
		errs: errs,
	}

	return s
}

func (s *Service) GetNumbers(w http.ResponseWriter, r *http.Request) {

	table := "number"
	limit := 300

	timeLimit := time.Now().AddDate(0, 0, -14)
	selectsql := fmt.Sprintf("SELECT t.* FROM 
	(
		SELECT * FROM %s
		WHERE id >
		(
			SELECT FLOOR(min_id + (max_id - min_id + 1) * RAND()) - %d 
			JOIN (SELECT min(id) As min_id, max(id) AS max_id FROM number )
		)
		AND is_deleted = FALSE AND ( reserve_id = 0 OR reserved_at > %d )
		ORDER BY version, number LIMIT 1 BY version, number
	) t
	LIMIT %d", table, limit, timeLimit, limit )

	rows, err := s.db.Query(selectsql)
	if !s.HandleError(err, w) {
		return
	}
	defer rows.Close()

	var result []

	for rows.Next() {
		p := &types.Number{}
		rows.Scan(&p.Id, &p.Number, &p.Beauty)
		result = append(result, p)
	}

	resp, err := json.Marshal( result )
	if !s.HandleError(err, w) {
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(resp)
}

func (s *Service) Reserve(w http.ResponseWriter, r *http.Request) {

	numbers := r.URL.Query().Get("numbers")
	if len(numbers) == 0 {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(resp))
	}

	table := "number"

	num_parts := make([]string, len(numbers), 0)

	for _, n := range numbers {
		num_parts = append(num_parts, n)
	}

	// Будем тут проверять на то, чтоб не брать удаленные номера?
	
	selectsql := fmt.Sprintf("SELECT id, number, beauty, created_at FROM %s WHERE number IN (%s) ORDER BY version LIMIT 1 BY number, version", table, strings.Join( num_parts, ", " ) )

	rows, err := s.db.Query(selectsql)
	if !s.HandleError(err, w) {
		return
	}
	defer rows.Close()

	// generate reserve code
	reserveId := fmt.Sprintf("%d%s", time.Now().Unix(), randString(10))
	reservedAt := time.Now()

	values := make([]string, len(numbers), 0)
	for rows.Next() {
		v := types.Number{ reserveId: reserveId, reservedAt: reservedAt }
		rows.Scan(&v.Id, &v.Number, &v.Beauty, &v.Created_at)
		nums = fmt.Sprintf("(%d, %d, %s, %d, %d)", v.Number, v.Reserved_at, v.ReserveId, v.Beauty, v.Created_at )
	}

	sql := fmt.Sprintf("INSERT INTO %s (number, reserved_at, reserved_id, beauty, created_at) VALUES %s",
	table, strings.Join( values, ", " ) )

	err := s.db.Exec(sql)
	if !s.HandleError(err, w) {
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(resp))
}


func (s *Service) GetReserved(w http.ResponseWriter, r *http.Request) {
	reserveId := r.URL.Query().Get("code")
	if reserveId == "" {
		s.HandleError(errors.New("empty code on getReserved request"), w)
		return
	}

	table := "number"
	limit := 300

	timeLimit := time.Now().AddDate(0, 0, -14)
	selectsql := fmt.Sprintf("SELECT number, reserved_at, reserve_id, beauty FROM %s
	AND is_deleted = FALSE AND reserve_id = %s AND reserved_at <= %d
	ORDER BY version, number LIMIT 1 BY version, number", table, limit, timeLimit, limit )

	rows, err := s.db.Query(selectsql)
	if !s.HandleError(err, w) {
		return
	}
	defer rows.Close()

	var result []

	for rows.Next() {
		p := &types.Number{}
		rows.Scan(&p.Number, &p.Reserved_at, &p.ReserveId, &p.Beauty)
		result = append(result, p)
	}

	resp, err := json.Marshal( result )
	if !s.HandleError(err, w) {
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(resp)
}

func (s *Service) HandleError(err error, w http.ResponseWriter) bool {
	if err != nil {
		s.errs <- err
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("%s", err)))
		return false
	}

	return true
}

func (s *Service) Shutdown(ctx context.Context) {
	return nil
}

func randString(length int) string {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ" +
		"abcdefghijklmnopqrstuvwxyzåäö" +
		"0123456789")
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}