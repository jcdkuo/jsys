# jsys

Jerry's local system command center.

## Run

```sh
npm start
```

Then open:

```text
http://localhost:4173
```

Set a different port if needed:

```sh
PORT=5000 npm start
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

The first version is zero-dependency: Node.js serves the app, samples the local
machine, and streams metrics to the browser through Server-Sent Events.
