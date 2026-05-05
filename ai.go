package main

import (
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func aiStatus() AIStatus {
	processes := processList()
	return AIStatus{
		Agents: []AIAgent{
			{Name: "Codex", Count: countCodexAgents(processes), Scope: "local"},
			{Name: "Cursor", Count: countCursorAgents(processes), Scope: "local"},
			{Name: "Claude", Count: countClaudeAgents(processes), Scope: "local"},
		},
		Remotes: remoteConnections(processes),
	}
}

func processList() []procInfo {
	output := run(1200*time.Millisecond, "ps", "-axo", "pid=,ppid=,command=")
	var processes []procInfo
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 3 {
			continue
		}
		pid := int(parseUint(fields[0]))
		ppid := int(parseUint(fields[1]))
		command := strings.Join(fields[2:], " ")
		processes = append(processes, procInfo{pid: pid, ppid: ppid, command: command})
	}
	return processes
}

func countCodexAgents(processes []procInfo) int {
	count := 0
	for _, process := range processes {
		if strings.Contains(process.command, "/codex/codex") {
			count++
		}
	}
	return count
}

func countCursorAgents(processes []procInfo) int {
	count := 0
	for _, process := range processes {
		if strings.Contains(process.command, "Cursor Helper (Plugin): extension-host (agent-exec)") {
			count++
		}
	}
	return count
}

func countClaudeAgents(processes []procInfo) int {
	count := 0
	for _, process := range processes {
		command := strings.ToLower(process.command)
		if !strings.Contains(command, "claude") && !strings.Contains(command, "anthropic") {
			continue
		}
		if strings.Contains(command, "/applications/claude.app/") || strings.Contains(command, "chrome-native-host") {
			continue
		}
		count++
	}
	return count
}

func remoteConnections(processes []procInfo) []RemoteConnection {
	byPID := map[int]procInfo{}
	for _, process := range processes {
		byPID[process.pid] = process
	}

	grouped := map[string]*RemoteConnection{}
	for _, process := range processes {
		args := strings.Fields(process.command)
		if len(args) == 0 || filepath.Base(args[0]) != "ssh" {
			continue
		}
		remote, ok := parseSSHCommand(args)
		if !ok {
			continue
		}
		remote.Source = remoteSource(process, byPID)
		key := remote.Target + "|" + remote.Port + "|" + remote.Source
		existing := grouped[key]
		if existing == nil {
			existing = &remote
			grouped[key] = existing
		}
		existing.Sessions++
		existing.PIDs = append(existing.PIDs, process.pid)
		if remote.Tunnel {
			existing.Tunnel = true
		}
	}

	remotes := make([]RemoteConnection, 0, len(grouped))
	for _, remote := range grouped {
		sort.Ints(remote.PIDs)
		remotes = append(remotes, *remote)
	}
	sort.Slice(remotes, func(i, j int) bool {
		if remotes[i].Sessions != remotes[j].Sessions {
			return remotes[i].Sessions > remotes[j].Sessions
		}
		return remotes[i].Target < remotes[j].Target
	})
	return remotes
}

func parseSSHCommand(args []string) (RemoteConnection, bool) {
	remote := RemoteConnection{Port: "22", Source: "terminal"}
	userFromOption := ""

	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			continue
		}
		if !strings.HasPrefix(arg, "-") {
			host := arg
			user := userFromOption
			if parts := strings.SplitN(host, "@", 2); len(parts) == 2 {
				user = parts[0]
				host = parts[1]
			}
			if host == "" || strings.Contains(host, "=") {
				continue
			}
			remote.Host = host
			remote.User = user
			remote.Target = host
			if user != "" {
				remote.Target = user + "@" + host
			}
			return remote, true
		}

		option := strings.TrimLeft(arg, "-")
		if option == "" {
			continue
		}
		if option[0] == 'D' {
			remote.Tunnel = true
		}
		if option[0] == 'p' || option[0] == 'l' || option[0] == 'D' || option[0] == 'L' || option[0] == 'R' || option[0] == 'i' || option[0] == 'F' || option[0] == 'o' || option[0] == 'J' || option[0] == 'b' || option[0] == 'c' || option[0] == 'm' || option[0] == 'S' {
			if len(option) > 1 {
				value := option[1:]
				if option[0] == 'p' {
					remote.Port = value
				}
				if option[0] == 'l' {
					userFromOption = value
				}
				continue
			}
			if i+1 >= len(args) {
				continue
			}
			value := args[i+1]
			if option[0] == 'p' {
				remote.Port = value
			}
			if option[0] == 'l' {
				userFromOption = value
			}
			i++
		}
	}

	return remote, false
}

func remoteSource(process procInfo, byPID map[int]procInfo) string {
	current := process
	for depth := 0; depth < 8; depth++ {
		parent, ok := byPID[current.ppid]
		if !ok {
			break
		}
		command := strings.ToLower(parent.command)
		if strings.Contains(command, "cursor") || strings.Contains(command, "cursor_remote_install") {
			return "Cursor Remote"
		}
		if strings.Contains(command, "sshpass") || strings.Contains(command, "/users/jerry.jckuo/bin/") {
			return "terminal"
		}
		current = parent
	}
	return "terminal"
}
