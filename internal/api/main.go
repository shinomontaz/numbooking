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
	selectsql := fmt.Sprintf("SELECT * FROM %s
	WHERE id >
	(
		SELECT FLOOR(min_id + (max_id - min_id + 1) * RAND()) - %d 
		JOIN (SELECT min(id) As min_id, max(id) AS max_id FROM number )
	)
	AND is_deleted = FALSE AND ( reserve_id = 0 OR reserved_at > %d )
	ORDER BY version, number LIMIT %d BY version, number", table, limit, timeLimit, limit )

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
	q, _ := ioutil.ReadAll(r.Body)
	ss := string(q)

	if !s.HandleError(err, w) {
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(resp))
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
