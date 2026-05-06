package main

import (
	"strings"
	"testing"
)

func TestParseSSHCommand(t *testing.T) {
	cases := []struct {
		name       string
		cmd        string
		wantOK     bool
		wantUser   string
		wantHost   string
		wantPort   string
		wantTunnel bool
	}{
		{"bare host", "ssh example.com", true, "", "example.com", "22", false},
		{"user at host", "ssh jerry@example.com", true, "jerry", "example.com", "22", false},
		{"port via -p flag", "ssh -p 2222 jerry@example.com", true, "jerry", "example.com", "2222", false},
		{"port via combined -p2222", "ssh -p2222 jerry@example.com", true, "jerry", "example.com", "2222", false},
		{"user via -l flag", "ssh -l jerry example.com", true, "jerry", "example.com", "22", false},
		{"identity file then host", "ssh -i ~/.ssh/id host", true, "", "host", "22", false},
		{"dynamic tunnel", "ssh -D 1080 jerry@host", true, "jerry", "host", "22", true},
		{"after double dash", "ssh -- host", true, "", "host", "22", false},
		{"no host found", "ssh", false, "", "", "22", false},
		{"only flags, no host", "ssh -v", false, "", "", "22", false},
		{"jump host -J", "ssh -J bastion host", true, "", "host", "22", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, ok := parseSSHCommand(strings.Fields(c.cmd))
			if ok != c.wantOK {
				t.Fatalf("ok=%v, want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			if r.User != c.wantUser {
				t.Errorf("user=%q, want %q", r.User, c.wantUser)
			}
			if r.Host != c.wantHost {
				t.Errorf("host=%q, want %q", r.Host, c.wantHost)
			}
			if r.Port != c.wantPort {
				t.Errorf("port=%q, want %q", r.Port, c.wantPort)
			}
			if r.Tunnel != c.wantTunnel {
				t.Errorf("tunnel=%v, want %v", r.Tunnel, c.wantTunnel)
			}
		})
	}
}

func TestParseSSHCommandTargetFormat(t *testing.T) {
	r, ok := parseSSHCommand(strings.Fields("ssh jerry@example.com"))
	if !ok {
		t.Fatal("expected parse success")
	}
	if r.Target != "jerry@example.com" {
		t.Errorf("Target=%q, want jerry@example.com", r.Target)
	}

	r, ok = parseSSHCommand(strings.Fields("ssh example.com"))
	if !ok {
		t.Fatal("expected parse success")
	}
	if r.Target != "example.com" {
		t.Errorf("Target=%q, want example.com (no user prefix)", r.Target)
	}
}
