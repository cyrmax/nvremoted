package commands

import (
	"fmt"
	"time"

	"github.com/n0ot/nvremoted/pkg/server"
	"github.com/spf13/viper"
)

// Explicit typed getters keep existing files compatible and avoid accepting
// typos, fractional/negative limits, or malformed duration strings silently.
func readAdmissionConfig(v *viper.Viper) (server.AdmissionConfig, error) {
	var cfg server.AdmissionConfig
	limits := map[string]*int{
		"hardLimit": &cfg.HardLimit, "pendingLimit": &cfg.PendingLimit,
		"tlsLimit": &cfg.TLSLimit, "startupLimit": &cfg.StartupLimit,
		"admittedLimit": &cfg.AdmittedLimit,
	}
	for name, target := range limits {
		key := "admission." + name
		if !v.IsSet(key) {
			continue
		}
		value := v.Get(key)
		switch n := value.(type) {
		case int:
			*target = n
		case int64:
			if int64(int(n)) != n {
				return cfg, fmt.Errorf("%s exceeds platform integer range", key)
			}
			*target = int(n)
		default:
			return cfg, fmt.Errorf("%s must be an integer", key)
		}
		if *target <= 0 {
			return cfg, fmt.Errorf("%s must be positive", key)
		}
	}
	durations := map[string]*time.Duration{
		"recoveryGrace": &cfg.RecoveryGrace, "startupTimeout": &cfg.StartupTimeout,
		"rejectionWindow": &cfg.RejectionWindow, "rejectionInterval": &cfg.RejectionInterval,
		"overlapTimeout": &cfg.OverlapTimeout,
	}
	for name, target := range durations {
		key := "admission." + name
		if !v.IsSet(key) {
			continue
		}
		raw, ok := v.Get(key).(string)
		if !ok {
			return cfg, fmt.Errorf("%s must be a duration string, e.g. 10s", key)
		}
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return cfg, fmt.Errorf("%s must be a positive duration", key)
		}
		*target = d
	}
	if v.IsSet("admission.protectedChannels") {
		switch values := v.Get("admission.protectedChannels").(type) {
		case []string:
			cfg.ProtectedChannels = append([]string(nil), values...)
		case []interface{}:
			for _, value := range values {
				s, ok := value.(string)
				if !ok {
					return cfg, fmt.Errorf("admission.protectedChannels must contain SHA-256 strings")
				}
				cfg.ProtectedChannels = append(cfg.ProtectedChannels, s)
			}
		default:
			return cfg, fmt.Errorf("admission.protectedChannels must be an array of SHA-256 strings")
		}
	}
	// Reject unknown options so a misspelled protection setting cannot appear
	// successful while the operator's intended guarantee is absent.
	for _, key := range v.AllKeys() {
		if len(key) < 10 || key[:10] != "admission." {
			continue
		}
		known := key == "admission.protectedchannels"
		for name := range limits {
			if key == "admission."+lowerASCII(name) {
				known = true
			}
		}
		for name := range durations {
			if key == "admission."+lowerASCII(name) {
				known = true
			}
		}
		if !known {
			return cfg, fmt.Errorf("unknown admission option %s", key)
		}
	}
	return cfg, cfg.Validate()
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
