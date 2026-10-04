package commands

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestAdmissionTOMLCompatibilityAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		valid         bool
	}{
		{"old file", `[server]
bind="127.0.0.1:6837"`, true},
		{"configured", `[admission]
hardLimit=12
pendingLimit=4
tlsLimit=2
startupLimit=4
admittedLimit=8
recoveryGrace="60s"
protectedChannels=["` + strings.Repeat("ab", 32) + `"]`, true},
		{"negative", "[admission]\npendingLimit=-1", false},
		{"fractional", "[admission]\npendingLimit=1.5", false},
		{"duration number", "[admission]\nstartupTimeout=5", false},
		{"duration negative", "[admission]\nstartupTimeout=\"-5s\"", false},
		{"zero", "[admission]\nhardLimit=0", false},
		{"invalid digest", "[admission]\nprotectedChannels=[\"secret-key\"]", false},
		{"overcommit", "[admission]\nhardLimit=3", false},
		{"typo", "[admission]\nprotectedChannel=[\"anything\"]", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("toml")
			if err := v.ReadConfig(strings.NewReader(tc.content)); err != nil {
				t.Fatal(err)
			}
			cfg, err := readAdmissionConfig(v)
			if (err == nil) != tc.valid {
				t.Fatalf("config=%+v error=%v", cfg, err)
			}
			if tc.name == "configured" && cfg.RecoveryGrace != time.Minute {
				t.Fatal(cfg)
			}
		})
	}
}
