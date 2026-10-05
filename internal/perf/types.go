// Package perf contains local benchmark tooling, never imported by the relay.
package perf

import "time"

const SchemaVersion = 1

type Config struct {
	Preset      string        `json:"preset"`
	Duration    time.Duration `json:"duration_ns"`
	Warmup      time.Duration `json:"warmup_ns"`
	Drain       time.Duration `json:"drain_ns"`
	Rate        int           `json:"rate_override"`
	Channels    int           `json:"channels_override"`
	Fanout      int           `json:"fanout_override"`
	Clients     int           `json:"clients_override"`
	Payload     string        `json:"payload_override"`
	Transport   string        `json:"transport"`
	Filter      string        `json:"filter"`
	Repetitions int           `json:"repetitions"`
	MaxInFlight int           `json:"max_in_flight"`
	Output      string        `json:"-"`
	Profile     string        `json:"profile,omitempty"`
}

// Scenario describes offered load, not a performance assertion. Rate is total
// messages/s across all senders. Clients is used for lifecycle/idle scenarios.
type Scenario struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Transport  string `json:"transport"`
	Payload    string `json:"payload"`
	Channels   int    `json:"channels"`
	Fanout     int    `json:"fanout"`
	Clients    int    `json:"clients"`
	Rate       int    `json:"target_messages_per_second"`
	Mixed      bool   `json:"mixed_hot_cold,omitempty"`
	Joined     bool   `json:"joined,omitempty"`
	Repetition int    `json:"repetition"`
}

// All latency values are nanoseconds. Quantiles use bounded logarithmic bins;
// min/max/mean/stddev are calculated from every sample without approximation.
type Distribution struct {
	Count  uint64   `json:"count"`
	Min    float64  `json:"min_ns"`
	Mean   float64  `json:"mean_ns"`
	P50    float64  `json:"p50_ns"`
	P90    float64  `json:"p90_ns"`
	P95    float64  `json:"p95_ns"`
	P99    float64  `json:"p99_ns"`
	P999   *float64 `json:"p99_9_ns,omitempty"`
	Max    float64  `json:"max_ns"`
	StdDev float64  `json:"stddev_ns"`
}

type RecipientResult struct {
	Channel          int          `json:"channel"`
	Recipient        int          `json:"recipient"`
	Received         uint64       `json:"received"`
	ReceivedInWindow uint64       `json:"received_in_measurement_window"`
	Missing          uint64       `json:"missing"`
	Latency          Distribution `json:"latency"`
	ScheduledLatency Distribution `json:"scheduled_latency"`
}

type SenderResult struct {
	Channel      int          `json:"channel"`
	Sent         uint64       `json:"sent"`
	SentInWindow uint64       `json:"sent_in_measurement_window"`
	Bytes        uint64       `json:"bytes"`
	WriteLatency Distribution `json:"write_latency"`
	GeneratorLag Distribution `json:"generator_lag"`
}

type Result struct {
	Scenario             Scenario          `json:"scenario"`
	DurationNS           int64             `json:"measurement_wall_ns"`
	DrainNS              int64             `json:"drain_wall_ns"`
	Offered              uint64            `json:"offered"`
	Sent                 uint64            `json:"sent"`
	SentInWindow         uint64            `json:"sent_in_measurement_window"`
	Received             uint64            `json:"received_deliveries"`
	ReceivedInWindow     uint64            `json:"received_in_measurement_window"`
	Expected             uint64            `json:"expected_deliveries"`
	Missing              uint64            `json:"missing_deliveries"`
	Duplicates           uint64            `json:"duplicates"`
	OutOfOrder           uint64            `json:"out_of_order"`
	Corrupted            uint64            `json:"corrupted"`
	Unexpected           uint64            `json:"unexpected"`
	Disconnects          uint64            `json:"disconnects"`
	GeneratorMissed      uint64            `json:"generator_missed"`
	TimestampLost        uint64            `json:"timestamp_lost"`
	SendBytes            uint64            `json:"send_bytes"`
	SendBytesInWindow    uint64            `json:"send_bytes_in_window"`
	ReceiveBytes         uint64            `json:"receive_bytes"`
	ReceiveBytesInWindow uint64            `json:"receive_bytes_in_measurement_window"`
	Latency              Distribution      `json:"latency"`
	ScheduledLatency     Distribution      `json:"scheduled_latency"`
	SendLatency          Distribution      `json:"sender_write_latency"`
	Completion           Distribution      `json:"last_recipient_completion"`
	GeneratorLag         Distribution      `json:"generator_lag"`
	Recipients           []RecipientResult `json:"recipients,omitempty"`
	Senders              []SenderResult    `json:"senders,omitempty"`
	Before               Resources         `json:"resources_before"`
	After                Resources         `json:"resources_after"`
	Resources            ResourceDelta     `json:"resources_delta"`
	QueueMax             int               `json:"sampled_max_event_queue"`
	QueueCapacity        int               `json:"event_queue_capacity"`
	QueueSamples         uint64            `json:"queue_samples"`
	OverflowDisconnects  uint64            `json:"queue_overflow_disconnects"`
	ServerErrors         uint64            `json:"server_errors"`
	ServerConfig         map[string]any    `json:"server_configuration,omitempty"`
	Observations         map[string]any    `json:"observations,omitempty"`
	Notes                []string          `json:"notes,omitempty"`
	Errors               []string          `json:"errors,omitempty"`
	Valid                bool              `json:"valid"`
}

type Metadata struct {
	StartedUTC   time.Time     `json:"started_utc"`
	OS           string        `json:"os"`
	Architecture string        `json:"architecture"`
	CPUModel     string        `json:"cpu_model"`
	LogicalCPUs  int           `json:"logical_cpus"`
	GoVersion    string        `json:"go_version"`
	Commit       string        `json:"commit"`
	Dirty        bool          `json:"dirty"`
	GOMAXPROCS   int           `json:"gomaxprocs"`
	CGO          string        `json:"cgo"`
	GOAMD64      string        `json:"goamd64,omitempty"`
	GOARM64      string        `json:"goarm64,omitempty"`
	GOEXPERIMENT string        `json:"goexperiment,omitempty"`
	Instrumented bool          `json:"instrumented"`
	Clock        ClockMetadata `json:"clock"`
}

type Report struct {
	SchemaVersion int      `json:"schema_version"`
	Metadata      Metadata `json:"metadata"`
	Config        Config   `json:"config"`
	Results       []Result `json:"results"`
	Notes         []string `json:"notes,omitempty"`
}
