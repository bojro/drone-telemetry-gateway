package model

// Reading represents a single telemetry reading from a sensor/drone. Contains what device publishes, what queue carries, what store persists.
type Reading struct {
	DeviceID    string  `json:"device_id"`
	Battery     float64 `json:"battery"`     // 0-100
	Temperature float64 `json:"temperature"` // celsius
	Velocity    float64 `json:"velocity"`    // m/s
	Timestamp   int64   `json:"timestamp"`   // unix seconds
}

// Valid reports whether the reading is acceptable and, if not, why. The reason
// string feeds a metric/log so we can see which rule failed.
func (r Reading) Valid() (bool, string) {
	if r.DeviceID == "" {
		return false, "missing device_id"
	}
	if r.Timestamp <= 0 {
		return false, "bad timestamp"
	}
	if r.Battery < 0 || r.Battery > 100 {
		return false, "battery out of range"
	}
	if r.Temperature < -50 || r.Temperature > 150 {
		return false, "temperature out of range"
	}
	if r.Velocity < 0 || r.Velocity > 1000 {
		return false, "velocity out of range"
	}

	return true, ""
}
