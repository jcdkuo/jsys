package main

import (
	"testing"
)

func TestParseUint(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"42", 42},
		{"  42  ", 42},
		{"42.", 42},
		{"", 0},
		{"abc", 0},
		{"-1", 0},
		{"18446744073709551615", 18446744073709551615},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := parseUint(c.in); got != c.want {
				t.Fatalf("parseUint(%q) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestParseFloat(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"3.14", 3.14},
		{" 2.5 ", 2.5},
		{"", 0},
		{"nope", 0},
		{"-1.5", -1.5},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			if got := parseFloat(c.in); got != c.want {
				t.Fatalf("parseFloat(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestClamp(t *testing.T) {
	cases := []struct {
		v, lo, hi, want float64
	}{
		{50, 0, 100, 50},
		{-5, 0, 100, 0},
		{150, 0, 100, 100},
		{0, 0, 100, 0},
		{100, 0, 100, 100},
	}
	for _, c := range cases {
		if got := clamp(c.v, c.lo, c.hi); got != c.want {
			t.Errorf("clamp(%v, %v, %v) = %v, want %v", c.v, c.lo, c.hi, got, c.want)
		}
	}
}

func TestPercent(t *testing.T) {
	cases := []struct {
		used, total uint64
		want        float64
	}{
		{50, 100, 50},
		{0, 100, 0},
		{100, 100, 100},
		{50, 0, 0}, // div-by-zero guard
		{0, 0, 0},
	}
	for _, c := range cases {
		if got := percent(c.used, c.total); got != c.want {
			t.Errorf("percent(%d, %d) = %v, want %v", c.used, c.total, got, c.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
		{2199023255552, "2.0 TB"},
	}
	for _, c := range cases {
		if got := formatBytes(c.in); got != c.want {
			t.Errorf("formatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestEnv(t *testing.T) {
	t.Setenv("JSYS_TEST_KEY", "value")
	if got := env("JSYS_TEST_KEY", "fallback"); got != "value" {
		t.Errorf("env returned %q, want %q", got, "value")
	}
	t.Setenv("JSYS_TEST_KEY", "  spaced  ")
	if got := env("JSYS_TEST_KEY", "fallback"); got != "spaced" {
		t.Errorf("env didn't trim: %q", got)
	}
	t.Setenv("JSYS_TEST_KEY", "")
	if got := env("JSYS_TEST_KEY", "fallback"); got != "fallback" {
		t.Errorf("env empty value should return fallback, got %q", got)
	}
}
