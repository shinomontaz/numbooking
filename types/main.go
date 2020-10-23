package types

import "time"

type DidNumber struct {
	ID           int64
	Number       string
	IsDeleted    bool
	BookingCode  string
	ReservedAt   time.Time
	ReservedTill time.Time
	// selected_at DateTime('UTC'), /* время выбора номера для просмотра */
	// selected_till DateTime('UTC'), /* время сокрытия номера без брони из поисковой выдачи */
	beauty int64
	region int64

	Provider int64 // provider ID
	Type     int
}
