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
- Git branch and working tree state
- Event stream for pressure, hot processes, and storage thresholds

The backend is a zero-dependency Go service. It serves the browser UI, samples
the local machine, and streams metrics through Server-Sent Events.
