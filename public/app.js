const historyLimit = 90;
const SERIES_STORAGE_KEY = "jsys.series.v1";
const series = {
  cpu: [],
  memory: [],
  network: [],
  load: []
};

function loadSeriesFromStorage() {
  try {
    const raw = localStorage.getItem(SERIES_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    for (const key of Object.keys(series)) {
      if (Array.isArray(parsed[key])) {
        series[key] = parsed[key].slice(-historyLimit).filter((v) => Number.isFinite(v));
      }
    }
  } catch {
    // ignore — corrupt or unavailable storage shouldn't break the page
  }
}

function saveSeriesToStorage() {
  try {
    localStorage.setItem(SERIES_STORAGE_KEY, JSON.stringify(series));
  } catch {
    // ignore quota / privacy mode
  }
}

const elements = {
  hostName: document.querySelector("#hostName"),
  connectionBadge: document.querySelector("#connectionBadge"),
  aiSummary: document.querySelector("#aiSummary"),
  clock: document.querySelector("#clock"),
  uptime: document.querySelector("#uptime"),
  platform: document.querySelector("#platform"),
  arch: document.querySelector("#arch"),
  nodeVersion: document.querySelector("#nodeVersion"),
  gitStatus: document.querySelector("#gitStatus"),
  ports: document.querySelector("#ports"),
  healthState: document.querySelector("#healthState"),
  healthScore: document.querySelector("#healthScore"),
  cpuValue: document.querySelector("#cpuValue"),
  memoryValue: document.querySelector("#memoryValue"),
  diskValue: document.querySelector("#diskValue"),
  networkValue: document.querySelector("#networkValue"),
  memoryDetail: document.querySelector("#memoryDetail"),
  networkDetail: document.querySelector("#networkDetail"),
  coreGrid: document.querySelector("#coreGrid"),
  disks: document.querySelector("#disks"),
  events: document.querySelector("#events"),
  eventCount: document.querySelector("#eventCount"),
  processes: document.querySelector("#processes"),
  aiAgents: document.querySelector("#aiAgents"),
  remotes: document.querySelector("#remotes"),
  remoteCount: document.querySelector("#remoteCount"),
  loadAverage: document.querySelector("#loadAverage")
};

const canvases = {
  core: document.querySelector("#coreCanvas"),
  cpu: document.querySelector("#cpuChart"),
  memory: document.querySelector("#memoryChart"),
  network: document.querySelector("#networkChart"),
  load: document.querySelector("#loadChart")
};

const colors = {
  cpu: "#31d7d3",
  memory: "#d56bff",
  network: "#70f29b",
  load: "#f3b54a",
  critical: "#ff5b6e",
  warn: "#f3b54a",
  stable: "#31d7d3"
};

let latestSample = null;
let corePhase = 0;
let lastMetricsAt = 0;
let reconnectDelay = 0;
let connectionState = "connecting";
let activeSource = null;

function setConnState(state) {
  if (connectionState === state) return;
  connectionState = state;
  const label = { live: "Live", stale: "Stale", reconnecting: "Reconnecting", connecting: "Connecting" }[state] || state;
  elements.connectionBadge.textContent = label;
  elements.connectionBadge.classList.toggle("badge--warn", state !== "live");
}

function connect() {
  setConnState(connectionState === "connecting" ? "connecting" : "reconnecting");
  const source = new EventSource("/events");
  activeSource = source;

  source.addEventListener("ready", () => {
    reconnectDelay = 0;
    setConnState("live");
  });

  source.addEventListener("metrics", (event) => {
    lastMetricsAt = Date.now();
    setConnState("live");
    latestSample = JSON.parse(event.data);
    render(latestSample);
  });

  source.onerror = () => {
    if (activeSource !== source) return;
    source.close();
    activeSource = null;
    setConnState("reconnecting");
    reconnectDelay = Math.min(reconnectDelay ? reconnectDelay * 2 : 5000, 60000);
    setTimeout(connect, reconnectDelay);
  };
}

setInterval(() => {
  if (connectionState === "live" && lastMetricsAt && Date.now() - lastMetricsAt > 3000) {
    setConnState("stale");
  }
}, 1000);

function render(sample) {
  push(series.cpu, sample.cpu.total);
  push(series.memory, sample.memory.percent);
  push(series.network, bytesToMegabits(sample.network.rxRate + sample.network.txRate));
  push(series.load, sample.cpu.loadAverage[0] || 0);

  elements.hostName.textContent = sample.host.name;
  elements.uptime.textContent = formatDuration(sample.host.uptime);
  elements.platform.textContent = sample.host.platform;
  elements.arch.textContent = sample.host.arch;
  elements.nodeVersion.textContent = sample.host.node;
  elements.gitStatus.textContent = `${sample.git.branch} / ${sample.git.clean ? "clean" : `${sample.git.changedFiles} changed`}`;

  elements.healthState.textContent = sample.health.state;
  elements.healthScore.textContent = Math.round(sample.health.score);
  elements.cpuValue.textContent = `${sample.cpu.total.toFixed(0)}%`;
  elements.memoryValue.textContent = `${sample.memory.percent.toFixed(0)}%`;
  elements.memoryDetail.textContent = `${formatBytes(sample.memory.used)} used / ${formatBytes(sample.memory.total)} total, swap ${sample.memory.swap.percent.toFixed(0)}%`;

  const primaryDisk = sample.disks.find((disk) => disk.mount === "/") || sample.disks[0];
  elements.diskValue.textContent = primaryDisk ? `${primaryDisk.percent}%` : "--%";

  const totalNetwork = sample.network.rxRate + sample.network.txRate;
  elements.networkValue.textContent = `${formatRate(totalNetwork)}/s`;
  elements.networkDetail.textContent = `Down ${formatRate(sample.network.rxRate)}/s / Up ${formatRate(sample.network.txRate)}/s`;
  elements.loadAverage.textContent = sample.cpu.loadAverage.map((value) => value.toFixed(2)).join(" / ");

  renderPorts(sample.ports);
  renderCores(sample.cpu.perCore, !!sample.cpu.perCoreEstimated);
  renderDisks(sample.disks);
  renderEvents(sample.events);
  renderProcesses(sample.processes);
  renderAI(sample.ai);
  drawLineChart(canvases.cpu, series.cpu, { max: 100, color: colors.cpu });
  drawLineChart(canvases.memory, series.memory, { max: 100, color: colors.memory });
  drawLineChart(canvases.network, series.network, { max: Math.max(1, ...series.network) * 1.25, color: colors.network });
  drawLineChart(canvases.load, series.load, { max: Math.max(sample.cpu.cores, ...series.load, 1), color: colors.load, fill: true });

  saveSeriesToStorage();
}

function renderAI(ai) {
  const agents = ai?.agents || [];
  const remotes = ai?.remotes || [];
  const sessionCount = remotes.reduce((sum, remote) => sum + remote.sessions, 0);
  const codex = agents.find((agent) => agent.name === "Codex")?.count ?? 0;
  const cursor = agents.find((agent) => agent.name === "Cursor")?.count ?? 0;
  const claude = agents.find((agent) => agent.name === "Claude")?.count ?? 0;
  elements.aiSummary.textContent = `AI Cx ${codex} / Cu ${cursor} / Cl ${claude} / R ${sessionCount}`;
  elements.remoteCount.textContent = `${sessionCount} remote`;

  elements.aiAgents.replaceChildren(
    ...agents.map((agent) => {
      const node = document.createElement("div");
      node.className = "agent-pill";
      node.innerHTML = `
        <span>${escapeHtml(agent.name)}</span>
        <strong>${agent.count}</strong>
        <small>${escapeHtml(agent.scope)}</small>
      `;
      return node;
    })
  );

  elements.remotes.replaceChildren(
    ...(remotes.length ? remotes : [{ target: "no-active-ssh", source: "remote", sessions: 0, pids: [] }]).map((remote) => {
      const node = document.createElement("div");
      node.className = "remote-row";
      const meta = remote.sessions
        ? `${remote.source} / ${remote.sessions} session${remote.sessions === 1 ? "" : "s"}${remote.tunnel ? " / tunnel" : ""}`
        : "no live remote links";
      node.innerHTML = `
        <strong title="${escapeHtml(remote.target)}">${escapeHtml(remote.target)}</strong>
        <span>${escapeHtml(meta)}</span>
      `;
      return node;
    })
  );
}

function renderPorts(ports) {
  elements.ports.replaceChildren(
    ...(ports.length ? ports : ["no-listeners"]).map((port) => {
      const node = document.createElement("span");
      node.textContent = typeof port === "number" ? `:${port}` : port;
      return node;
    })
  );
}

function renderCores(cores, estimated) {
  elements.coreGrid.dataset.estimated = estimated ? "true" : "false";

  const cpuHeader = document.querySelector('article[data-metric="cpu"] .metric-card__header');
  const existingTag = cpuHeader.querySelector(".estimated-tag");
  if (estimated && !existingTag) {
    const tag = document.createElement("small");
    tag.className = "estimated-tag";
    tag.textContent = "(estimated)";
    cpuHeader.appendChild(tag);
  } else if (!estimated && existingTag) {
    existingTag.remove();
  }

  if (elements.coreGrid.children.length !== cores.length) {
    elements.coreGrid.replaceChildren(
      ...cores.map(() => {
        const block = document.createElement("span");
        block.className = "core-block";
        const fill = document.createElement("i");
        fill.className = "core-fill";
        block.appendChild(fill);
        return block;
      })
    );
  }

  cores.forEach((value, i) => {
    const block = elements.coreGrid.children[i];
    block.title = `${value.toFixed(1)}%`;
    const fill = block.firstChild;
    fill.style.height = `${value}%`;
    fill.style.background = heatColor(value);
  });
}

function renderDisks(disks) {
  elements.disks.replaceChildren(
    ...disks.slice(0, 4).map((disk) => {
      const row = document.createElement("div");
      row.className = "disk-row";
      row.innerHTML = `
        <div class="disk-row__label"><span>${escapeHtml(disk.mount)}</span><span>${disk.percent}%</span></div>
        <div class="bar"><i style="width:${clamp(disk.percent, 0, 100)}%;"></i></div>
      `;
      return row;
    })
  );
}

function renderEvents(events) {
  elements.eventCount.textContent = events.length;

  const existing = new Map();
  for (const child of elements.events.children) {
    if (child.dataset.eventId) existing.set(Number(child.dataset.eventId), child);
  }

  const fragment = document.createDocumentFragment();
  for (const item of events) {
    let node = existing.get(item.id);
    if (node) {
      existing.delete(item.id);
    } else {
      node = document.createElement("li");
      node.dataset.eventId = item.id;
      node.dataset.level = item.level;
      node.classList.add("event-new");
      node.innerHTML = `
        <div>
          <time>${new Date(item.time).toLocaleTimeString()}</time>
          <strong>${escapeHtml(item.title)}</strong>
          <p>${escapeHtml(item.detail)}</p>
        </div>
      `;
      setTimeout(() => node.classList.remove("event-new"), 700);
    }
    fragment.appendChild(node);
  }

  elements.events.replaceChildren(fragment);
}

function renderProcesses(processes) {
  elements.processes.replaceChildren(
    ...processes.map((process) => {
      const node = document.createElement("div");
      node.className = "process-row";
      node.innerHTML = `
        <strong title="${escapeHtml(process.command)}">${escapeHtml(process.command)}</strong>
        <span>${process.cpu.toFixed(1)}%</span>
        <span>${process.memory.toFixed(1)}%</span>
      `;
      return node;
    })
  );
}

function drawLineChart(canvas, values, options) {
  const context = scaleCanvas(canvas);
  const { width, height } = canvas.getBoundingClientRect();
  context.clearRect(0, 0, width, height);
  drawGrid(context, width, height);

  if (values.length < 2) return;

  const max = options.max || 100;
  const points = values.map((value, index) => ({
    x: (index / (historyLimit - 1)) * width,
    y: height - (clamp(value, 0, max) / max) * (height - 14) - 7
  }));

  context.beginPath();
  points.forEach((point, index) => {
    if (index === 0) context.moveTo(point.x, point.y);
    else context.lineTo(point.x, point.y);
  });

  context.lineWidth = 2.5;
  context.strokeStyle = options.color;
  context.shadowColor = options.color;
  context.shadowBlur = 14;
  context.stroke();
  context.shadowBlur = 0;

  if (options.fill) {
    context.lineTo(width, height);
    context.lineTo(0, height);
    context.closePath();
    const gradient = context.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, `${options.color}66`);
    gradient.addColorStop(1, `${options.color}00`);
    context.fillStyle = gradient;
    context.fill();
  }
}

function drawCore() {
  const canvas = canvases.core;
  const context = scaleCanvas(canvas);
  const rect = canvas.getBoundingClientRect();
  const width = rect.width;
  const height = rect.height;
  const centerX = width / 2;
  const centerY = height / 2;
  const score = latestSample?.health.score || 0;
  const state = latestSample?.health.state || "Stable";
  const accent = state === "Critical" ? colors.critical : state === "Pressure" ? colors.warn : colors.stable;

  corePhase += 0.012 + score / 11000;
  context.clearRect(0, 0, width, height);

  const rings = 5;
  for (let i = 0; i < rings; i += 1) {
    const radius = width * (0.18 + i * 0.075);
    context.beginPath();
    context.arc(centerX, centerY, radius, corePhase * (i % 2 ? -1 : 1), Math.PI * 1.58 + corePhase * (i % 2 ? -1 : 1));
    context.lineWidth = i === 0 ? 9 : 3;
    context.strokeStyle = hexToRgba(i % 2 ? "#d56bff" : accent, 0.35 + i * 0.08);
    context.shadowColor = accent;
    context.shadowBlur = 18;
    context.stroke();
  }

  const spokes = 56;
  for (let i = 0; i < spokes; i += 1) {
    const angle = (i / spokes) * Math.PI * 2 + corePhase;
    const pulse = 0.55 + Math.sin(corePhase * 4 + i) * 0.28 + score / 220;
    const inner = width * 0.34;
    const outer = width * (0.39 + pulse * 0.05);
    context.beginPath();
    context.moveTo(centerX + Math.cos(angle) * inner, centerY + Math.sin(angle) * inner);
    context.lineTo(centerX + Math.cos(angle) * outer, centerY + Math.sin(angle) * outer);
    context.lineWidth = 1;
    context.strokeStyle = hexToRgba(accent, 0.22 + pulse * 0.2);
    context.stroke();
  }

  const gradient = context.createRadialGradient(centerX, centerY, 0, centerX, centerY, width * 0.25);
  gradient.addColorStop(0, hexToRgba(accent, 0.38));
  gradient.addColorStop(0.62, "rgba(255,255,255,0.035)");
  gradient.addColorStop(1, "rgba(255,255,255,0)");
  context.fillStyle = gradient;
  context.beginPath();
  context.arc(centerX, centerY, width * 0.25, 0, Math.PI * 2);
  context.fill();

  requestAnimationFrame(drawCore);
}

function drawGrid(context, width, height) {
  context.strokeStyle = "rgba(255,255,255,0.07)";
  context.lineWidth = 1;
  for (let i = 1; i < 4; i += 1) {
    const y = (height / 4) * i;
    context.beginPath();
    context.moveTo(0, y);
    context.lineTo(width, y);
    context.stroke();
  }
}

function scaleCanvas(canvas) {
  const ratio = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const width = Math.max(Math.floor(rect.width), 1);
  const height = Math.max(Math.floor(rect.height), 1);
  if (canvas.width !== width * ratio || canvas.height !== height * ratio) {
    canvas.width = width * ratio;
    canvas.height = height * ratio;
  }
  const context = canvas.getContext("2d");
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  return context;
}

function push(values, value) {
  values.push(Number.isFinite(value) ? value : 0);
  if (values.length > historyLimit) values.shift();
}

function bytesToMegabits(bytes) {
  return (bytes * 8) / 1024 / 1024;
}

function formatRate(bytes) {
  return formatBytes(bytes);
}

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
}

function formatDuration(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days) return `${days}d ${hours}h`;
  if (hours) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function heatColor(value) {
  if (value > 82) return colors.critical;
  if (value > 62) return colors.warn;
  return colors.cpu;
}

function hexToRgba(hex, alpha) {
  const value = hex.replace("#", "");
  const red = parseInt(value.slice(0, 2), 16);
  const green = parseInt(value.slice(2, 4), 16);
  const blue = parseInt(value.slice(4, 6), 16);
  return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  })[char]);
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

setInterval(() => {
  elements.clock.textContent = new Date().toLocaleTimeString();
}, 500);

loadSeriesFromStorage();
connect();
drawCore();
