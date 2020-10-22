package types

import "time"

type Number struct {
	ID         int       `json:"id"`
	Number     int       `json:"number"`
	Beauty     int       `json:"beauty"`
	CreatedAt  time.Time `json:"created_at"`
	ReservedAt time.Time `json:"reserved_at"`
	ReserveId  string    `json:"reserve_id"`
	IsDeleted  bool
	Version    time.Time
}
