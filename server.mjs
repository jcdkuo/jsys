import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { createReadStream, existsSync } from "node:fs";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";
import { execFile } from "node:child_process";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const publicDir = join(__dirname, "public");
const port = Number(process.env.PORT || 4173);
const host = process.env.HOST || "127.0.0.1";
const clients = new Set();

let previousCpu = snapshotCpu();
let previousNet = null;
let previousNetTime = Date.now();
let eventId = 0;
const recentEvents = [];

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml; charset=utf-8",
  ".ico": "image/x-icon"
};

function run(command, args, timeout = 1200) {
  return new Promise((resolve) => {
    execFile(command, args, { timeout }, (error, stdout) => {
      resolve(error ? "" : stdout.trim());
    });
  });
}

function snapshotCpu() {
  return os.cpus().map((cpu) => ({ ...cpu.times }));
}

function cpuUsage() {
  const current = snapshotCpu();
  const perCore = current.map((now, index) => {
    const before = previousCpu[index] || now;
    const idle = now.idle - before.idle;
    const total = Object.keys(now).reduce((sum, key) => sum + now[key] - before[key], 0);
    return total > 0 ? clamp(((total - idle) / total) * 100, 0, 100) : 0;
  });
  previousCpu = current;

  const total = perCore.length
    ? perCore.reduce((sum, value) => sum + value, 0) / perCore.length
    : 0;

  return {
    total,
    perCore,
    cores: perCore.length,
    model: os.cpus()[0]?.model || "Unknown CPU",
    loadAverage: os.loadavg()
  };
}

async function memoryUsage() {
  const total = os.totalmem();
  const free = os.freemem();
  const used = total - free;
  const swap = await swapUsage();

  return {
    total,
    used,
    free,
    percent: total > 0 ? (used / total) * 100 : 0,
    swap
  };
}

async function swapUsage() {
  if (process.platform === "darwin") {
    const output = await run("sysctl", ["-n", "vm.swapusage"]);
    const total = Number(output.match(/total = ([\d.]+)M/)?.[1] || 0) * 1024 * 1024;
    const used = Number(output.match(/used = ([\d.]+)M/)?.[1] || 0) * 1024 * 1024;
    return { total, used, percent: total > 0 ? (used / total) * 100 : 0 };
  }

  if (existsSync("/proc/meminfo")) {
    const output = await readFile("/proc/meminfo", "utf8").catch(() => "");
    const total = Number(output.match(/^SwapTotal:\s+(\d+)/m)?.[1] || 0) * 1024;
    const free = Number(output.match(/^SwapFree:\s+(\d+)/m)?.[1] || 0) * 1024;
    const used = Math.max(total - free, 0);
    return { total, used, percent: total > 0 ? (used / total) * 100 : 0 };
  }

  return { total: 0, used: 0, percent: 0 };
}

async function diskUsage() {
  const output = await run("df", ["-kP"]);
  return output
    .split("\n")
    .slice(1)
    .map((line) => line.trim().split(/\s+/))
    .filter((parts) => parts.length >= 6)
    .filter((parts) => !parts[0].startsWith("devfs"))
    .map((parts) => ({
      filesystem: parts[0],
      size: Number(parts[1]) * 1024,
      used: Number(parts[2]) * 1024,
      available: Number(parts[3]) * 1024,
      percent: Number(parts[4].replace("%", "")),
      mount: parts.slice(5).join(" ")
    }))
    .slice(0, 7);
}

async function networkUsage() {
  const now = Date.now();
  const current = process.platform === "linux"
    ? await linuxNetworkCounters()
    : await bsdNetworkCounters();
  const interval = Math.max((now - previousNetTime) / 1000, 0.25);
  const before = previousNet || current;
  previousNet = current;
  previousNetTime = now;

  const interfaces = Object.entries(current)
    .filter(([name]) => !/^lo/.test(name))
    .map(([name, stats]) => {
      const old = before[name] || stats;
      return {
        name,
        rxBytes: stats.rxBytes,
        txBytes: stats.txBytes,
        rxRate: Math.max((stats.rxBytes - old.rxBytes) / interval, 0),
        txRate: Math.max((stats.txBytes - old.txBytes) / interval, 0)
      };
    })
    .sort((a, b) => b.rxRate + b.txRate - (a.rxRate + a.txRate));

  const rxRate = interfaces.reduce((sum, item) => sum + item.rxRate, 0);
  const txRate = interfaces.reduce((sum, item) => sum + item.txRate, 0);
  return { rxRate, txRate, interfaces: interfaces.slice(0, 5) };
}

async function linuxNetworkCounters() {
  const output = await readFile("/proc/net/dev", "utf8").catch(() => "");
  const counters = {};
  for (const line of output.split("\n").slice(2)) {
    const [rawName, rawStats] = line.split(":");
    if (!rawName || !rawStats) continue;
    const parts = rawStats.trim().split(/\s+/).map(Number);
    counters[rawName.trim()] = { rxBytes: parts[0] || 0, txBytes: parts[8] || 0 };
  }
  return counters;
}

async function bsdNetworkCounters() {
  const output = await run("netstat", ["-ibn"]);
  const counters = {};
  for (const line of output.split("\n").slice(1)) {
    const parts = line.trim().split(/\s+/);
    if (parts.length < 10) continue;
    const name = parts[0];
    const rxBytes = Number(parts[6]);
    const txBytes = Number(parts[9]);
    if (!Number.isFinite(rxBytes) || !Number.isFinite(txBytes)) continue;
    const current = counters[name] || { rxBytes: 0, txBytes: 0 };
    counters[name] = {
      rxBytes: Math.max(current.rxBytes, rxBytes),
      txBytes: Math.max(current.txBytes, txBytes)
    };
  }
  return counters;
}

async function topProcesses() {
  const output = await run("ps", ["-axo", "pid=,comm=,%cpu=,%mem=", "-r"]);
  return output
    .split("\n")
    .map((line) => line.trim().match(/^(\d+)\s+(.+?)\s+([\d.]+)\s+([\d.]+)$/))
    .filter(Boolean)
    .map((match) => ({
      pid: Number(match[1]),
      command: match[2].split("/").pop() || `pid ${match[1]}`,
      cpu: Number(match[3]),
      memory: Number(match[4])
    }))
    .slice(0, 8);
}

async function openPorts() {
  const output = process.platform === "darwin"
    ? await run("lsof", ["-nP", "-iTCP", "-sTCP:LISTEN"], 1000)
    : await run("ss", ["-ltnp"], 1000);

  const ports = new Set();
  for (const line of output.split("\n").slice(1)) {
    const match = line.match(/[:.](\d+)(?:\s|\)|$)/g);
    if (!match) continue;
    const last = match.at(-1)?.match(/\d+/)?.[0];
    if (last) ports.add(Number(last));
  }
  return [...ports].sort((a, b) => a - b).slice(0, 16);
}

async function gitStatus() {
  const branch = await run("git", ["branch", "--show-current"]);
  const short = await run("git", ["status", "--short"]);
  return {
    branch: branch || "unknown",
    changedFiles: short ? short.split("\n").filter(Boolean).length : 0,
    clean: !short
  };
}

async function sampleSystem() {
  const [memory, disks, network, processes, ports, git] = await Promise.all([
    memoryUsage(),
    diskUsage(),
    networkUsage(),
    topProcesses(),
    openPorts(),
    gitStatus()
  ]);
  const cpu = cpuUsage();
  const primaryDisk = disks.find((disk) => disk.mount === "/") || disks[0] || { percent: 0 };
  const pressure = calculatePressure(cpu, memory, primaryDisk, network);
  const events = generateEvents({ cpu, memory, disks, network, processes, pressure });

  return {
    timestamp: Date.now(),
    host: {
      name: os.hostname(),
      platform: `${os.type()} ${os.release()}`,
      arch: os.arch(),
      uptime: os.uptime(),
      node: process.version
    },
    health: pressure,
    cpu,
    memory,
    disks,
    network,
    processes,
    ports,
    git,
    events
  };
}

function calculatePressure(cpu, memory, disk, network) {
  const loadPressure = cpu.cores ? clamp((cpu.loadAverage[0] / cpu.cores) * 100, 0, 100) : 0;
  const networkPressure = clamp(Math.log10(1 + network.rxRate + network.txRate) * 11, 0, 100);
  const score = clamp(
    cpu.total * 0.34 + memory.percent * 0.28 + disk.percent * 0.2 + loadPressure * 0.14 + networkPressure * 0.04,
    0,
    100
  );
  const state = score >= 82 ? "Critical" : score >= 62 ? "Pressure" : "Stable";
  return { score, state };
}

function generateEvents(sample) {
  const now = Date.now();
  const candidates = [];

  if (sample.cpu.total > 85) candidates.push(["critical", "CPU saturation", `${sample.cpu.total.toFixed(0)}% utilization across ${sample.cpu.cores} cores`]);
  else if (sample.cpu.total > 65) candidates.push(["warn", "CPU pressure", `${sample.cpu.total.toFixed(0)}% utilization trend detected`]);

  if (sample.memory.percent > 88) candidates.push(["critical", "Memory ceiling", `${formatBytes(sample.memory.used)} used of ${formatBytes(sample.memory.total)}`]);
  else if (sample.memory.percent > 72) candidates.push(["warn", "Memory pressure", `${sample.memory.percent.toFixed(0)}% RAM allocation`]);

  for (const disk of sample.disks.filter((item) => item.percent > 80).slice(0, 2)) {
    candidates.push([disk.percent > 90 ? "critical" : "warn", "Storage threshold", `${disk.mount} is ${disk.percent}% full`]);
  }

  const topProcess = sample.processes[0];
  if (topProcess?.cpu > 60) candidates.push(["warn", "Hot process", `${topProcess.command} is using ${topProcess.cpu.toFixed(0)}% CPU`]);

  if (!candidates.length && recentEvents.length === 0) {
    candidates.push(["info", "Telemetry online", "Live system stream established"]);
  }

  for (const [level, title, detail] of candidates) {
    const key = `${level}:${title}:${detail}`;
    if (recentEvents[0]?.key === key) continue;
    recentEvents.unshift({ id: ++eventId, key, level, title, detail, time: now });
  }

  recentEvents.splice(12);
  return recentEvents.map(({ key, ...event }) => event);
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
}

function sendEvent(response, event, data) {
  response.write(`event: ${event}\n`);
  response.write(`data: ${JSON.stringify(data)}\n\n`);
}

async function broadcastSample() {
  if (!clients.size) return;
  const sample = await sampleSystem();
  for (const response of clients) {
    sendEvent(response, "metrics", sample);
  }
}

setInterval(() => {
  broadcastSample().catch((error) => {
    console.error("Failed to sample system metrics:", error);
  });
}, 1000);

const server = createServer(async (request, response) => {
  const url = new URL(request.url || "/", `http://${request.headers.host}`);

  if (url.pathname === "/events") {
    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no"
    });
    clients.add(response);
    sendEvent(response, "ready", { ok: true });
    sampleSystem().then((sample) => sendEvent(response, "metrics", sample));
    request.on("close", () => clients.delete(response));
    return;
  }

  if (url.pathname === "/api/snapshot") {
    const sample = await sampleSystem();
    response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
    response.end(JSON.stringify(sample));
    return;
  }

  const requestedPath = url.pathname === "/" ? "/index.html" : decodeURIComponent(url.pathname);
  const safePath = normalize(requestedPath).replace(/^(\.\.[/\\])+/, "");
  const filePath = join(publicDir, safePath);

  if (!filePath.startsWith(publicDir)) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }

  if (!existsSync(filePath)) {
    response.writeHead(404);
    response.end("Not found");
    return;
  }

  response.writeHead(200, {
    "Content-Type": mimeTypes[extname(filePath)] || "application/octet-stream"
  });
  createReadStream(filePath).pipe(response);
});

server.on("error", (error) => {
  console.error(`Unable to start jsys on ${host}:${port}:`, error.message);
  process.exit(1);
});

server.listen(port, host, () => {
  console.log(`jsys command center: http://${host}:${port}`);
});
