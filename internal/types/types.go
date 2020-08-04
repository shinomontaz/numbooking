package types

import "time"

type Number struct {
	Number      int       `json:"number"`
	Beauty      int       `json:"beauty"`
	Created_at  time.Time `json:"created_at"`
	Reserved_at time.Time `json:"reserved_at"`
	ReserveId   int       `json:"reserve_id"`
	IsDeleted   bool
	Version     time.Time
}
