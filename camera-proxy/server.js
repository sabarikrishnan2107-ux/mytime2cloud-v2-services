const { WebSocketServer, WebSocket } = require("ws");
const { spawn } = require("child_process");
const net = require("net");
const axios = require("axios");

const PORT = 8501;
const LARAVEL_API = "https://v2backend.mytime2cloud.com/api";
const LARAVEL_TOKEN = "";
const BACKPRESSURE_LIMIT = 512 * 1024;
const RESTART_DELAY_MS = 2000;
const IDLE_TEARDOWN_MS = 10000;
const DEFAULT_RTSP_PORTS = [554, 8554, 10554];
const DEFAULT_RTSP_PATHS = [
  "/Streaming/Channels/101",
  "/cam/realmonitor?channel=1&subtype=0",
  "/h264Preview_01_main",
  "/live/ch00_0",
  "/live/main",
  "/stream1",
  "/11",
];
const SOI = Buffer.from([0xff, 0xd8]);
const EOI = Buffer.from([0xff, 0xd9]);

const HEARTBEAT_INTERVAL_MS = 25000; // 25 seconds

const wss = new WebSocketServer({ port: PORT, host: "0.0.0.0" });
const deviceStreams = new Map();

// Heartbeat: ping all clients every 25s, terminate dead ones
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL_MS);

console.log(`Camera proxy running on ws://0.0.0.0:${PORT}`);

function parseDeviceId(pathname = "") {
  const segments = pathname.split("/").filter(Boolean);
  return segments[segments.length - 1] || null;
}

function broadcastJson(stream, payload) {
  const message = JSON.stringify(payload);

  for (const client of stream.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function broadcastFrame(stream, frame) {
  for (const client of stream.clients) {
    if (client.readyState === WebSocket.OPEN && client.bufferedAmount < BACKPRESSURE_LIMIT) {
      client.send(frame);
    }
  }
}

async function fetchCameraCredentials(deviceId) {
  const headers = {};
  if (LARAVEL_TOKEN) {
    headers.Authorization = `Bearer ${LARAVEL_TOKEN}`;
  }

  const { data } = await axios.get(`${LARAVEL_API}/camera/${deviceId}/credentials`, { headers });

  if (!data.status) {
    throw new Error("Camera not found");
  }

  return data.data;
}

function normalizeRtspPath(path = "") {
  const value = String(path || "").trim();

  if (!value) {
    return "";
  }

  if (value.startsWith("rtsp://")) {
    return value;
  }

  return value.startsWith("/") ? value : `/${value}`;
}

function getRtspPorts(creds) {
  const configuredPort = Number(creds.camera_rtsp_port);

  if (configuredPort > 0) {
    return [configuredPort];
  }

  return DEFAULT_RTSP_PORTS;
}

function buildRtspAuthSegment(creds, portOverride = null) {
  const ip = creds.camera_rtsp_ip;
  const port = portOverride || creds.camera_rtsp_port || 554;
  const user = creds.camera_username || "";
  const pass = creds.camera_password || "";

  if (user && pass) {
    return `rtsp://${encodeURIComponent(user)}:${encodeURIComponent(pass)}@${ip}:${port}`;
  }

  if (user) {
    return `rtsp://${encodeURIComponent(user)}@${ip}:${port}`;
  }

  return `rtsp://${ip}:${port}`;
}

function buildRtspUrlFromPath(creds, path, portOverride = null) {
  const normalizedPath = normalizeRtspPath(path);

  if (!normalizedPath) {
    return null;
  }

  if (normalizedPath.startsWith("rtsp://")) {
    return normalizedPath;
  }

  return `${buildRtspAuthSegment(creds, portOverride)}${normalizedPath}`;
}

function buildRtspCandidates(creds) {
  const configuredPath = creds.camera_rtsp_path || "";
  const normalizedConfiguredPath = normalizeRtspPath(configuredPath);
  const candidates = [];
  const seen = new Set();
  const ports = getRtspPorts(creds);

  const addCandidate = (path, port, baseCreds = creds) => {
    const url = buildRtspUrlFromPath(baseCreds, path, port);
    if (!url || seen.has(url)) {
      return;
    }
    seen.add(url);
    candidates.push(url);
  };

  if (normalizedConfiguredPath.startsWith("rtsp://")) {
    addCandidate(normalizedConfiguredPath, null);

    try {
      const parsed = new URL(normalizedConfiguredPath);
      const parsedPort = Number(parsed.port || 0);
      const uniquePorts = [...new Set(parsedPort > 0 ? [parsedPort, ...ports] : ports)];

      const candidateCreds = {
        ...creds,
        camera_rtsp_ip: creds.camera_rtsp_ip || parsed.hostname,
        camera_username: creds.camera_username || parsed.username,
        camera_password: creds.camera_password || parsed.password,
      };

      if (candidateCreds.camera_rtsp_ip) {
        const parsedPath = `${parsed.pathname || ""}${parsed.search || ""}`.trim();

        if (parsedPath && parsedPath !== "/") {
          for (const port of uniquePorts) {
            addCandidate(parsedPath, port, candidateCreds);
          }
        }

        for (const port of uniquePorts) {
          for (const path of DEFAULT_RTSP_PATHS) {
            addCandidate(path, port, candidateCreds);
          }
        }
      }
    } catch (error) {
      // Ignore parse errors; fall back to using the full URL only.
    }

    return candidates;
  }

  for (const port of ports) {
    addCandidate(configuredPath, port);
  }

  for (const port of ports) {
    for (const path of DEFAULT_RTSP_PATHS) {
      addCandidate(path, port);
    }
  }

  return candidates;
}

function maskRtspUrl(rtspUrl = "") {
  return String(rtspUrl || "").replace(/\/\/([^:/@]+):([^@]+)@/g, "//$1:***@");
}

function getOrCreateStream(deviceId) {
  if (!deviceStreams.has(deviceId)) {
    deviceStreams.set(deviceId, {
      deviceId,
      clients: new Set(),
      ffmpeg: null,
      buffer: Buffer.alloc(0),
      starting: false,
      restartTimer: null,
      idleTimer: null,
      shouldRestart: true,
      credentials: null,
      rtspCandidates: [],
      rtspCandidateIndex: 0,
      activeRtspUrl: null,
      receivedFrameForAttempt: false,
    });
  }

  return deviceStreams.get(deviceId);
}

function clearStreamTimers(stream) {
  if (stream.restartTimer) {
    clearTimeout(stream.restartTimer);
    stream.restartTimer = null;
  }

  if (stream.idleTimer) {
    clearTimeout(stream.idleTimer);
    stream.idleTimer = null;
  }
}

function stopStream(stream) {
  stream.shouldRestart = false;
  clearStreamTimers(stream);

  if (stream.ffmpeg && !stream.ffmpeg.killed) {
    stream.ffmpeg.kill("SIGTERM");
  }

  stream.ffmpeg = null;
  stream.buffer = Buffer.alloc(0);
}

function getHostPortFromRtspUrl(rtspUrl) {
  try {
    const parsed = new URL(rtspUrl);
    return {
      host: parsed.hostname,
      port: Number(parsed.port || 554),
    };
  } catch (error) {
    return null;
  }
}

function checkTcpReachable(host, port, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let settled = false;

    const finish = (isReachable) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(isReachable);
    };

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));

    try {
      socket.connect(port, host);
    } catch (error) {
      finish(false);
    }
  });
}

async function findReachableRtspTargets(rtspCandidates) {
  const uniqueTargets = [];
  const seen = new Set();

  for (const rtspUrl of rtspCandidates) {
    const target = getHostPortFromRtspUrl(rtspUrl);
    if (!target) {
      continue;
    }

    const key = `${target.host}:${target.port}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    uniqueTargets.push(target);
  }

  const reachable = [];
  for (const target of uniqueTargets) {
    const ok = await checkTcpReachable(target.host, target.port);
    if (ok) {
      reachable.push(target);
    }
  }

  return {
    uniqueTargets,
    reachable,
  };
}

function buildReconnectMessage(reason, stream) {
  if (typeof reason !== "string" || !reason.trim()) {
    return "Camera stream reconnecting...";
  }

  if (reason.startsWith("rtsp_unreachable:")) {
    const detail = reason.replace("rtsp_unreachable:", "").trim();
    return `Camera not reachable: ${detail}. Retrying...`;
  }

  if (reason.startsWith("ffmpeg_exit_")) {
    return "Camera stream disconnected. Retrying...";
  }

  return `Camera stream reconnecting: ${reason}`;
}

function scheduleRestart(stream, reason) {
  if (!stream.shouldRestart || stream.clients.size === 0 || stream.restartTimer) {
    return;
  }

  console.warn(`Scheduling restart for device ${stream.deviceId}: ${reason}`);
  broadcastJson(stream, {
    status: "reconnecting",
    message: buildReconnectMessage(reason, stream),
    reason,
  });

  stream.restartTimer = setTimeout(() => {
    stream.restartTimer = null;
    ensureStreamRunning(stream);
  }, RESTART_DELAY_MS);
}

function scheduleTeardown(stream) {
  if (stream.clients.size > 0 || stream.idleTimer) {
    return;
  }

  stream.idleTimer = setTimeout(() => {
    console.log(`Stopping idle stream for device ${stream.deviceId}`);
    stopStream(stream);
    deviceStreams.delete(stream.deviceId);
  }, IDLE_TEARDOWN_MS);
}

async function ensureStreamRunning(stream) {
  if (stream.ffmpeg || stream.starting || stream.clients.size === 0) {
    return;
  }

  stream.starting = true;
  stream.shouldRestart = true;
  clearStreamTimers(stream);

  try {
    const creds = await fetchCameraCredentials(stream.deviceId);
    stream.credentials = creds;
    stream.rtspCandidates = buildRtspCandidates(creds);

    if (stream.rtspCandidates.length === 0) {
      throw new Error("No RTSP path configured and no fallback RTSP candidates available");
    }

    const { uniqueTargets, reachable } = await findReachableRtspTargets(stream.rtspCandidates);
    if (uniqueTargets.length > 0 && reachable.length === 0) {
      const portsTried = [...new Set(uniqueTargets.map((target) => target.port))].join(", ");
      throw new Error(
        `rtsp_unreachable:${creds.camera_rtsp_ip} is not accepting connections on ports ${portsTried}`
      );
    }

    if (stream.activeRtspUrl && stream.rtspCandidates.includes(stream.activeRtspUrl)) {
      stream.rtspCandidateIndex = stream.rtspCandidates.indexOf(stream.activeRtspUrl);
    } else if (stream.rtspCandidateIndex >= stream.rtspCandidates.length) {
      stream.rtspCandidateIndex = 0;
    }

    const rtspUrl = stream.rtspCandidates[stream.rtspCandidateIndex];
    stream.activeRtspUrl = rtspUrl;
    stream.receivedFrameForAttempt = false;

    console.log(
      `Connecting shared stream for device ${stream.deviceId} using ${maskRtspUrl(rtspUrl)}`
    );

    const ffmpeg = spawn("ffmpeg", [
      "-rtsp_transport", "tcp",
      "-fflags", "nobuffer",
      "-flags", "low_delay",
      "-i", rtspUrl,
      "-f", "mjpeg",
      "-q:v", "3",
      "-r", "20",
      "-vf", "scale=1280:-1",
      "-an",
      "pipe:1",
    ]);

    stream.ffmpeg = ffmpeg;
    stream.buffer = Buffer.alloc(0);

    ffmpeg.on("error", (err) => {
      console.error(`FFmpeg spawn error for device ${stream.deviceId}:`, err.message);
      stream.ffmpeg = null;
      broadcastJson(stream, { error: "FFmpeg not found. Please install FFmpeg." });
      scheduleRestart(stream, err.message);
    });

    ffmpeg.stdout.on("data", (chunk) => {
      stream.buffer = Buffer.concat([stream.buffer, chunk]);

      while (true) {
        const soiIndex = stream.buffer.indexOf(SOI);
        const eoiIndex = stream.buffer.indexOf(EOI);

        if (soiIndex === -1 || eoiIndex === -1 || eoiIndex < soiIndex) {
          break;
        }

        const frame = stream.buffer.subarray(soiIndex, eoiIndex + 2);
        stream.buffer = stream.buffer.subarray(eoiIndex + 2);
        if (!stream.receivedFrameForAttempt) {
          stream.receivedFrameForAttempt = true;
          broadcastJson(stream, { status: "streaming", message: "Camera stream active" });
        }
        broadcastFrame(stream, frame);
      }
    });

    ffmpeg.stderr.on("data", (data) => {
      const message = maskRtspUrl(data.toString());
      if (message.includes("Error") || message.includes("error") || message.includes("Connection")) {
        console.error(`FFmpeg ${stream.deviceId}: ${message.trim()}`);
      }
    });

    ffmpeg.on("close", (code) => {
      if (stream.ffmpeg === ffmpeg) {
        stream.ffmpeg = null;
      }
      stream.buffer = Buffer.alloc(0);

      console.warn(`FFmpeg exited for device ${stream.deviceId} with code ${code}`);

      if (!stream.receivedFrameForAttempt && stream.rtspCandidates.length > 1) {
        stream.rtspCandidateIndex = (stream.rtspCandidateIndex + 1) % stream.rtspCandidates.length;
        stream.activeRtspUrl = stream.rtspCandidates[stream.rtspCandidateIndex];
        console.warn(
          `No frames received for device ${stream.deviceId}. Switching to alternate RTSP candidate ${stream.rtspCandidateIndex + 1}/${stream.rtspCandidates.length}`
        );
      }

      if (stream.shouldRestart && stream.clients.size > 0) {
        scheduleRestart(stream, `ffmpeg_exit_${code}`);
      }
    });
  } catch (err) {
    console.error(`Failed to start stream for device ${stream.deviceId}:`, err.message);
    broadcastJson(stream, { error: err.message || "Failed to fetch camera credentials" });
    scheduleRestart(stream, err.message || "credential_fetch_failed");
  } finally {
    stream.starting = false;
  }
}

wss.on("connection", async (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  const deviceId = parseDeviceId(req.url);

  if (!deviceId) {
    ws.send(JSON.stringify({ error: "No device ID provided" }));
    ws.close();
    return;
  }

  const stream = getOrCreateStream(deviceId);
  stream.clients.add(ws);
  clearStreamTimers(stream);

  console.log(`Client connected for device ${deviceId}. Total clients: ${stream.clients.size}`);

  ws.send(JSON.stringify({ status: "connecting", message: "Connecting to camera..." }));
  ensureStreamRunning(stream);

  const removeClient = () => {
    stream.clients.delete(ws);
    console.log(`Client disconnected for device ${deviceId}. Remaining clients: ${stream.clients.size}`);

    if (stream.clients.size === 0) {
      scheduleTeardown(stream);
    }
  };

  ws.on("close", removeClient);
  ws.on("error", removeClient);
});
