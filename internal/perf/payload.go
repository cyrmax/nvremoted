package perf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func PayloadNames() []string {
	return []string{"key", "control", "gesture", "speak-short", "speak", "speak-large",
		"braille-20", "braille-40", "braille-80", "braille-160", "clipboard-1KiB",
		"clipboard-16KiB", "clipboard-64KiB", "clipboard-256KiB", "clipboard-1MiB",
		"arbitrary-flat", "arbitrary-nested", "arbitrary-strings", "arbitrary-arrays"}
}

// Payloads are synthetic examples of upstream NVDA wire shapes, not captured
// user speech or clipboard data. Unknown fields are valid relay payload fields.
func Payload(name string) ([]byte, error) {
	m := map[string]any{"bench_seq": uint64(0)}
	switch name {
	case "key":
		m["type"], m["vk_code"], m["extended"], m["pressed"] = "key", 65, false, true
	case "control":
		m["type"], m["switch"] = "pause_speech", true
	case "gesture":
		m["type"], m["dots"], m["space"], m["routingIndex"] = "braille_input", 21, false, 3
	case "speak-short", "speak", "speak-large":
		sequence := []any{"Ready"}
		if name != "speak-short" {
			sequence = []any{[]any{"PitchCommand", map[string]any{"offset": 10}}, "File list. ",
				[]any{"CharacterModeCommand", map[string]any{"state": true}}, "report.txt",
				[]any{"CharacterModeCommand", map[string]any{"state": false}}, " selected, 3 of 12.",
				[]any{"EndUtteranceCommand", map[string]any{}}}
		}
		if name == "speak-large" {
			unit := append([]any(nil), sequence...)
			for i := 0; i < 63; i++ {
				sequence = append(sequence, unit...)
			}
		}
		m["type"], m["sequence"], m["priority"] = "speak", sequence, "normal"
	case "arbitrary-flat":
		m["type"], m["enabled"], m["value"], m["text"] = "bench_arbitrary", true, 123, "sample"
	case "arbitrary-nested":
		m["type"], m["tree"] = "bench_arbitrary", map[string]any{"children": []any{map[string]any{"id": 1, "label": "first", "attributes": map[string]any{"active": true}}, map[string]any{"id": 2, "values": []any{1, 2, 3, nil}}}}
	case "arbitrary-strings":
		m["type"], m["text"] = "bench_arbitrary", strings.Repeat("Text with unicode: привет, 世界; quotes \" and newline\n", 32)
	case "arbitrary-arrays":
		values := make([]any, 256)
		for i := range values {
			values[i] = []any{i, fmt.Sprintf("item-%d", i), i%2 == 0}
		}
		m["type"], m["items"] = "bench_arbitrary", values
	case "mixed":
		return Payload("key") // Runner rotates templates; not a single mixed shape.
	default:
		if strings.HasPrefix(name, "braille-") {
			n, err := strconv.Atoi(strings.TrimPrefix(name, "braille-"))
			if err != nil || n < 1 || n > 4096 {
				return nil, fmt.Errorf("invalid braille profile %q", name)
			}
			cells := make([]int, n)
			for i := range cells {
				cells[i] = (i * 37) % 256
			}
			m["type"], m["cells"] = "display", cells
		} else if strings.HasPrefix(name, "clipboard-") {
			n, ok := map[string]int{"1KiB": 1024, "16KiB": 16384, "64KiB": 65536, "256KiB": 262144, "1MiB": 1048576}[strings.TrimPrefix(name, "clipboard-")]
			if !ok {
				return nil, fmt.Errorf("invalid clipboard profile %q", name)
			}
			m["type"], m["text"] = "set_clipboard_text", strings.Repeat("x", n)
		} else {
			return nil, fmt.Errorf("unknown payload profile %q", name)
		}
	}
	return json.Marshal(m)
}

type Template struct{ prefix, suffix []byte }

func NewTemplate(name string) (Template, error) {
	data, err := Payload(name)
	if err != nil {
		return Template{}, err
	}
	return TemplateFromJSON(data)
}

func TemplateFromJSON(data []byte) (Template, error) {
	needle := []byte(`"bench_seq":0`)
	i := bytes.Index(data, needle)
	if i < 0 || bytes.Count(data, needle) != 1 || !json.Valid(data) {
		return Template{}, fmt.Errorf("payload requires one bench_seq:0 placeholder")
	}
	return Template{prefix: append([]byte(nil), data[:i+len(needle)-1]...), suffix: append([]byte(nil), data[i+len(needle):]...)}, nil
}

func (t Template) AppendWire(dst []byte, seq uint64) []byte {
	dst = append(dst, t.prefix...)
	dst = strconv.AppendUint(dst, seq, 10)
	dst = append(dst, t.suffix...)
	return append(dst, '\n')
}

func (t Template) Wire(seq uint64) []byte { return t.AppendWire(nil, seq) }
