// Package event defines the immutable payload shared by durable delivery and analytics.
package event

import "time"

// Click identifies one accepted redirect request. Retries must preserve every field.
type Click struct {
	ID         string
	LinkID     int64
	Code       string
	UserAgent  string
	Referer    string
	OccurredAt time.Time
}
