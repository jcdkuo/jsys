# jsys

Jerry's local system command center.

## Run

```sh
go run .
```

Then open:

```text
http://localhost:4173
```

Set a different port if needed:

```sh
PORT=5000 go run .
```

Run on all network interfaces for remote access:

```sh
./scripts/run-remote.sh
```

Then open:

```text
http://<machine-ip>:9527
```

Build a single binary:

```sh
go build -o jsys .
./jsys
```

## What It Shows

- Live CPU pressure with per-core activity blocks
- RAM and swap usage
- Disk capacity by mount point
- Network throughput
- Top CPU and memory processes
- Listening ports
- Local Codex, Cursor, and Claude agent counts
- Active SSH remote links grouped by target and source
- Git branch and working tree state
- Event stream for pressure, hot processes, and storage thresholds

The backend is a zero-dependency Go service. It serves the browser UI, samples
the local machine, and streams metrics through Server-Sent Events.
