package types

import "time"

// FlowRetention stores the global flow event retention setting.
// It is a singleton row (ID is always 1); the dashboard can update it
// at runtime and the flow cleanup worker picks it up on its next cycle.
type FlowRetention struct {
	ID        uint `gorm:"primaryKey"`
	Retention time.Duration
	UpdatedAt time.Time
}
