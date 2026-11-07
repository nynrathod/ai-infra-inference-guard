package bench

import (
	"bytes"
	"strings"
	"testing"
)

func TestBarChartRendersLabelsAndValues(t *testing.T) {
	var buf bytes.Buffer
	err := WriteBarChartSVG(&buf, "Goodput by mode", "req/s", []Bar{
		{Label: "none", Value: 1.25, Sub: "120 tok/s"},
		{Label: "adaptive", Value: 3.5, Sub: "400 tok/s"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"<svg", "</svg>",
		"Goodput by mode",
		"none", "adaptive",
		"120 tok/s", "400 tok/s",
		"3.50", // formatted bar value
	} {
		if !strings.Contains(out, want) {
			t.Errorf("svg missing %q", want)
		}
	}
}

func TestBarChartEscapesXML(t *testing.T) {
	var buf bytes.Buffer
	err := WriteBarChartSVG(&buf, "a<b & c>", "y", []Bar{{Label: "mode<&>", Value: 1}})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "<b ") || strings.Contains(out, "<&>") {
		t.Errorf("svg contains unescaped characters: %s", out)
	}
}

func TestBarChartHandlesEmptyInput(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteBarChartSVG(&buf, "empty", "y", nil); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(buf.String(), "no data") {
		t.Errorf("empty chart should say so, got: %s", buf.String())
	}
}

func TestNiceCeil(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0.3, 0.5}, {1, 1}, {2.1, 5}, {5, 5}, {7.3, 10}, {13, 20}, {80, 100}, {0, 1},
	}
	for _, tc := range cases {
		if got := niceCeil(tc.in); got != tc.want {
			t.Errorf("niceCeil(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
