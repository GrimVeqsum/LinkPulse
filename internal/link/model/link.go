package model

import "time"

type Link struct {
	ID        int64
	Code      string
	TargetURL string
	CreatedAt time.Time
}
