/**
 * log-listener-mytime-mqtt-batch-sqlite.js
 *
 * SQLite version of the MQTT log listener for local desktop Windows setup.
 * Uses Node.js built-in node:sqlite (DatabaseSync) against the shared Laravel database.sqlite.
 *
 * Mirrors log-listener-mytime-mqtt-batch.js but replaces all PostgreSQL
 * calls with synchronous node:sqlite prepared statements.
 *
 * Features:
 * 1) INSERT ... ON CONFLICT DO NOTHING for dedup (requires UNIQUE index).
 * 2) Batches attendance inserts (queue + flush every FLUSH_MS or when MAX_BATCH reached).
 * 3) Caches device->company_id to avoid per-row query.
 * 4) Throttles heartbeat DB updates (writes at most once per device per MIN_HB_DB_WRITE_MS).
 *
 * Requirements: Node.js >= 22.5.0 (for node:sqlite built-in module)
 */

require("dotenv").config();
const mqtt = require("mqtt");
const { Aedes } = require("aedes");
const net = require("net");
const { DatabaseSync } = require("node:sqlite");
const fs = require("fs");
const path = require("path");


const os = require('os');

function getLocalIpAddress() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      // Skip over non-IPv4 and internal (i.e. 127.0.0.1) addresses
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1'; // Fallback to localhost if nothing found
}

const localIp = getLocalIpAddress();

// ========== ERROR LOGGING ==========

function todayGMT4() {
  const now = new Date();
  return new Date(now.toLocaleString("en-US", { timeZone: "Asia/Dubai" }));
}

function getErrorFile() {
  const today = todayGMT4().toISOString().slice(0, 10);
  return path.join(__dirname, `error-${today}.log`);
}

function logError(message) {
  const datetime = todayGMT4().toISOString();
  const line = `[${datetime}] ${message}\n`;
  try {
    fs.appendFileSync(getErrorFile(), line);
  } catch (e) {
    console.error("FAILED to write error log:", e.message);
  }
  console.error("ERROR:", message);
}

// Helper: sanitize info object for logging (remove big pic field)
function sanitizeInfo(info) {
  if (!info) return info;
  const clone = { ...info };
  if (clone.pic) clone.pic = "[BINARY_PIC_OMITTED]";
  return clone;
}

const uniqueId =
  "mqtt-loglistner-sqlite-mytim2cloud-" +
  Math.random().toString(36).substring(2, 10) +
  "-" +
  Date.now();

// ========== CONFIG ==========

// MQTT
const MQTT_HOST = `mqtt://${localIp}`;
const MQTT_PORT = 1883;
const MQTT_TOPIC_ATT = "mqtt/face/+/+";
const MQTT_TOPIC_HEARTBEAT = "mqtt/face/heartbeat";
const MODEL_NUMBER = process.env.MODEL_NUMBER || "MYTIME1";

// SQLite database path - defaults to Laravel's database
const SQLITE_DB_PATH =
  process.env.SQLITE_DB_PATH ||
  path.join(__dirname, "../backend/database/database.sqlite");

// CSV logs directory
const logDir =
  process.env.MYTIME_LOG_DIR ||
  path.join(__dirname, "./mqtt-mytime-logs");


// --- The "Nice" Console Log ---

console.log("\n" + "=".repeat(50));
console.log(`🚀 SERVICE STARTED: ${MODEL_NUMBER}`);
console.log("=".repeat(50));

console.table({
  "MQTT Host": MQTT_HOST,
  "MQTT Port": MQTT_PORT,
  "Attendance Topic": MQTT_TOPIC_ATT,
  "Heartbeat Topic": MQTT_TOPIC_HEARTBEAT,
  "SQLite DB": SQLITE_DB_PATH,
  "Log Directory": logDir
});

console.log("=".repeat(50));
console.log("📡 Listening for MQTT messages...\n");

// Ensure log dir exists
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

function getTodayFile() {
  const today = todayGMT4().toISOString().slice(0, 10);
  return path.join(logDir, `logs-${today}.csv`);
}

// ========== SQLITE DATABASE ==========

let db;
try {
  db = new DatabaseSync(SQLITE_DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA busy_timeout = 5000");
  db.exec("PRAGMA foreign_keys = ON");
  console.log("SQLite database opened:", SQLITE_DB_PATH);
} catch (err) {
  logError("Failed to open SQLite database: " + (err?.message || err));
  process.exit(1);
}

// Ensure the unique index for dedup exists
try {
  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS attendance_logs_uniq
    ON attendance_logs ("DeviceID", "LogTime", "UserID")
  `);
  console.log("Unique index attendance_logs_uniq ensured.");
} catch (err) {
  // Index creation fails if duplicate rows exist - clean them up first
  console.log("Unique index failed (duplicates exist). Cleaning up...");
  try {
    db.exec(`
      DELETE FROM attendance_logs
      WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM attendance_logs
        GROUP BY "DeviceID", "LogTime", "UserID"
      )
    `);
    console.log("Duplicate rows removed.");
    db.exec(`
      CREATE UNIQUE INDEX IF NOT EXISTS attendance_logs_uniq
      ON attendance_logs ("DeviceID", "LogTime", "UserID")
    `);
    console.log("Unique index attendance_logs_uniq created after cleanup.");
  } catch (err2) {
    logError("Failed to create unique index after cleanup: " + (err2?.message || err2));
  }
}

// ========== PREPARED STATEMENTS ==========

const stmtGetCompanyId = db.prepare(`
  SELECT company_id FROM devices
  WHERE serial_number = ? OR device_id = ?
  LIMIT 1
`);

const stmtSetDeviceOnline = db.prepare(`
  UPDATE devices
  SET status_id = 1
  WHERE model_number = ?
    AND (serial_number = ? OR device_id = ?)
`);

const stmtSetDeviceOffline = db.prepare(`
  UPDATE devices
  SET status_id = 2
  WHERE model_number = ?
    AND (serial_number = ? OR device_id = ?)
`);

const stmtHeartbeatUpdate = db.prepare(`
  UPDATE devices
  SET status_id = 1,
      last_live_datetime = ?
  WHERE model_number = ?
    AND (serial_number = ? OR device_id = ?)
`);

const stmtInsertAttendance = db.prepare(`
  INSERT INTO attendance_logs (
    "UserID",
    "DeviceID",
    "company_id",
    "LogTime",
    "SerialNumber",
    "status",
    "mode",
    "reason",
    "log_date_time",
    "index_serial_number",
    "log_date",
    "created_at",
    "updated_at"
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT ("DeviceID", "LogTime", "UserID") DO NOTHING
`);

const stmtBegin = db.prepare("BEGIN");
const stmtCommit = db.prepare("COMMIT");
const stmtRollback = db.prepare("ROLLBACK");

// ========== DEVICE COMPANY CACHE ==========

// Track last heartbeat info for devices (multi-device support)
const heartbeatMap = new Map();

// Device company cache (serial_number/device_id -> company_id)
const deviceCompanyCache = new Map();
const DEVICE_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
const deviceCompanyCacheTS = new Map();

function getCompanyIdForDevice(deviceId) {
  const key = String(deviceId || "").trim();
  if (!key) return null;

  const ts = deviceCompanyCacheTS.get(key) || 0;
  const fresh = Date.now() - ts < DEVICE_CACHE_TTL_MS;

  if (fresh && deviceCompanyCache.has(key)) {
    return deviceCompanyCache.get(key);
  }

  try {
    const row = stmtGetCompanyId.get(key, key);
    const cid = row?.company_id ?? null;
    deviceCompanyCache.set(key, cid);
    deviceCompanyCacheTS.set(key, Date.now());
    return cid;
  } catch (err) {
    logError(
      "getCompanyIdForDevice failed (" +
      key +
      "): " +
      (err?.message || err),
    );
    return null;
  }
}

// ========== HEARTBEAT STATUS CHECK (EVERY HOUR) ==========

function verifyDevicesOnlineStatus() {
  const now = Date.now();
  const ONE_HOUR_MS = 60 * 60 * 1000;

  if (heartbeatMap.size === 0) {
    console.log(
      "No heartbeat received for any device yet. Skipping status check.",
    );
    return;
  }

  for (const [serial, hbInfo] of heartbeatMap.entries()) {
    const { lastHeartbeatTs } = hbInfo || {};

    try {
      if (lastHeartbeatTs && now - lastHeartbeatTs <= ONE_HOUR_MS) {
        stmtSetDeviceOnline.run(MODEL_NUMBER, serial, serial);
        console.log(
          "Heartbeat OK in last hour. Setting device ONLINE:",
          serial,
          "| last HB:",
          new Date(lastHeartbeatTs).toISOString(),
        );
      } else {
        stmtSetDeviceOffline.run(MODEL_NUMBER, serial, serial);
        console.log(
          "No heartbeat in last hour. Setting device OFFLINE:",
          serial,
          "| last HB:",
          lastHeartbeatTs ? new Date(lastHeartbeatTs).toISOString() : "never",
        );
      }
    } catch (err) {
      logError(
        "Failed to update devices status (" +
        serial +
        "): " +
        (err?.message || err),
      );
    }
  }
}

// Run check every 1 hour
setInterval(() => {
  try {
    verifyDevicesOnlineStatus();
  } catch (err) {
    logError("verifyDevicesOnlineStatus error: " + (err?.message || err));
  }
}, 60 * 60 * 1000);

// ========== HEARTBEAT DB WRITE THROTTLE ==========
const lastHeartbeatDbWriteMap = new Map();
const MIN_HB_DB_WRITE_MS = 60 * 1000;

// ========== ATTENDANCE BATCH INSERT ==========
const attendanceQueue = [];
const MAX_QUEUE = 5000;
const MAX_BATCH = 300;
const FLUSH_MS = 300;

function flushAttendanceQueue() {
  if (attendanceQueue.length === 0) return;

  const batch = attendanceQueue.splice(0, MAX_BATCH);

  try {
    stmtBegin.run();
    for (const r of batch) {
      stmtInsertAttendance.run(
        r.UserID,
        r.DeviceID,
        r.company_id,
        r.LogTime,
        r.SerialNumber,
        r.status,
        r.mode,
        r.reason,
        r.log_date_time,
        r.index_serial_number,
        r.log_date,
        r.created_at,
        r.updated_at,
      );
    }
    stmtCommit.run();
    console.log(`SQLite batch inserted ${batch.length} attendance rows`);
  } catch (err) {
    try {
      stmtRollback.run();
    } catch (_) { }
    logError("Bulk attendance insert failed: " + (err?.message || err));
    // Fallback: write batch to a file for later reprocessing
    try {
      const fallbackFile = path.join(
        logDir,
        `failed-attendance-${todayGMT4().toISOString().slice(0, 10)}.jsonl`,
      );
      const lines = batch.map((b) => JSON.stringify(b)).join("\n") + "\n";
      fs.appendFileSync(fallbackFile, lines);
    } catch (e) {
      logError("Failed to write fallback attendance batch: " + e.message);
    }
  }
}

// Periodic flush
setInterval(() => {
  try {
    flushAttendanceQueue();
  } catch (err) {
    logError("flushAttendanceQueue error: " + (err?.message || err));
  }
}, FLUSH_MS);

// ========== LOCAL AEDES MQTT BROKER + MQTT CLIENT ==========
const LOCAL_BROKER_PORT = parseInt(MQTT_PORT, 10);

let aedes;
let brokerServer;
let client;

(async () => {
  aedes = await Aedes.createBroker();
  brokerServer = net.createServer(aedes.handle.bind(aedes));

  brokerServer.listen(LOCAL_BROKER_PORT, () => {
    console.log("Aedes MQTT broker listening on port", LOCAL_BROKER_PORT);
  });

  aedes.on("client", (aedesClient) => {
    console.log("Broker: client connected:", aedesClient.id);
  });

  aedes.on("clientDisconnect", (aedesClient) => {
    console.log("Broker: client disconnected:", aedesClient.id);
  });

  aedes.on("publish", (packet, aedesClient) => {
    if (aedesClient) {
      console.log(
        "Broker: message from",
        aedesClient.id,
        "on topic",
        packet.topic,
      );
    }
  });

  // ========== MQTT CLIENT (connects to local broker) ==========
  client = mqtt.connect(`${MQTT_HOST}:${LOCAL_BROKER_PORT}`, {
    clientId: `gateway-${uniqueId}`,
    keepalive: 30,
  });

  client.on("connect", () => {
    console.log("MQTT connected to", MQTT_HOST);

    client.subscribe([MQTT_TOPIC_ATT, MQTT_TOPIC_HEARTBEAT], (err) => {
      if (err) {
        logError("Subscribe error: " + err.message);
      } else {
        console.log(
          "Subscribed to:",
          MQTT_TOPIC_ATT,
          "and",
          MQTT_TOPIC_HEARTBEAT,
        );
      }
    });
  });

  client.on("error", (err) => logError("MQTT error: " + err.message));

  // ========== MQTT MESSAGE HANDLER ==========
  client.on("message", (receivedTopic, messageBuffer) => {
    console.log(
      "-----------------------------------------------------------LOG Listener (SQLite) - receivedTopic",
      receivedTopic,
    );

    // 1) HEARTBEAT MESSAGE
    if (receivedTopic === MQTT_TOPIC_HEARTBEAT) {
      let hbJson;

      try {
        hbJson = JSON.parse(messageBuffer.toString());
        console.log("hbJson", hbJson);
      } catch {
        logError("Invalid HeartBeat JSON payload: " + messageBuffer.toString());
        return;
      }

      if (!hbJson.info) {
        logError("HeartBeat JSON missing 'info' field");
        return;
      }

      const hbInfo = hbJson.info;
      const serialNumber = hbInfo.facesluiceId || null;
      const hbTimeStr = hbInfo.time || null;

      if (!serialNumber) {
        logError(
          "HeartBeat missing facesluiceId/serial_number. Payload: " +
          JSON.stringify(sanitizeInfo(hbInfo)),
        );
        return;
      }

      if (!hbTimeStr || hbTimeStr.trim() === "") {
        logError(
          "HeartBeat missing time field. Payload: " +
          JSON.stringify(sanitizeInfo(hbInfo)),
        );
        return;
      }

      // Track last heartbeat time for this device
      let lastHeartbeatTs = Date.now();
      const parsedMs = Date.parse(hbTimeStr);
      if (!isNaN(parsedMs)) lastHeartbeatTs = parsedMs;
      else logError("HeartBeat time parse failed, using now(). time=" + hbTimeStr);

      heartbeatMap.set(serialNumber, {
        lastHeartbeatTs,
        lastHeartbeatTimeStr: hbTimeStr,
      });

      console.log(
        "  Heartbeat received. Serial/device_id:",
        serialNumber,
        "| JSON time:",
        hbTimeStr,
        "| stored lastHeartbeatTs:",
        new Date(lastHeartbeatTs).toISOString(),
      );

      // Throttle DB updates for heartbeat
      const lastWrite = lastHeartbeatDbWriteMap.get(serialNumber) || 0;
      const shouldWrite = Date.now() - lastWrite >= MIN_HB_DB_WRITE_MS;

      if (!shouldWrite) return;

      try {
        stmtHeartbeatUpdate.run(
          hbTimeStr,
          MODEL_NUMBER,
          serialNumber,
          serialNumber,
        );

        lastHeartbeatDbWriteMap.set(serialNumber, Date.now());

        console.log(
          "Device ONLINE & last_live_datetime updated (throttled) (model:",
          MODEL_NUMBER,
          "key:",
          serialNumber + ")",
        );
      } catch (err) {
        logError(
          "Failed to update devices (heartbeat): " + (err?.message || err),
        );
      }

      return;
    }

    // 2) ATTENDANCE / OTHER FACE TOPICS
    let json;
    try {
      json = JSON.parse(messageBuffer.toString());
    } catch {
      logError("Invalid JSON payload: " + messageBuffer.toString());
      return;
    }

    if (!json.info) {
      logError("Missing .info field. Topic: " + receivedTopic);
      return;
    }

    const info = json.info;
    const safeInfo = sanitizeInfo(info);

    // Require RFIDCard or personId
    const userId = info.RFIDCard || info.personId || null;
    if (!userId || userId == 0 || userId < 0) {
      logError(
        "Skipping insert: No RFIDCard + No personId. Payload: " +
        JSON.stringify(safeInfo),
      );
      return;
    }

    const timeStr = info.time || null;
    if (!timeStr || timeStr.trim() === "") {
      logError(
        "Skipping insert: Missing timeStr. Payload: " + JSON.stringify(safeInfo),
      );
      return;
    }

    const logDate = timeStr.split(" ")[0];
    const deviceId = String(info.facesluiceId || "").trim();
    const logTime = info.time;

    if (!deviceId) {
      logError(
        "Skipping insert: Missing facesluiceId (DeviceID). Payload: " +
        JSON.stringify(safeInfo),
      );
      return;
    }

    // Company id (cached, synchronous)
    const companyId = getCompanyIdForDevice(deviceId);

    // Convert Date to string for SQLite
    const now = todayGMT4();
    const nowStr = now.toISOString().slice(0, 19).replace("T", " ");

    // Prepare row for batch insert
    const row = {
      UserID: userId,
      DeviceID: deviceId,
      company_id: companyId,
      LogTime: logTime,
      SerialNumber: info.RecordID || null,
      status: info.VerifyStatus == 1 ? "Allowed" : "Denied",
      mode: "Face",
      reason: "---",
      log_date_time: timeStr,
      index_serial_number: info.RecordID || null,
      log_date: logDate,
      created_at: nowStr,
      updated_at: nowStr,
    };

    // Queue safety (backpressure)
    if (attendanceQueue.length >= MAX_QUEUE) {
      try {
        const overflowFile = path.join(
          logDir,
          `overflow-attendance-${todayGMT4().toISOString().slice(0, 10)}.jsonl`,
        );
        fs.appendFileSync(overflowFile, JSON.stringify(row) + "\n");
        logError(
          `Attendance queue overflow (>${MAX_QUEUE}). Wrote to overflow file and dropped from memory.`,
        );
      } catch (e) {
        logError(
          `Attendance queue overflow and failed to write overflow file: ${e.message}`,
        );
      }
    } else {
      attendanceQueue.push(row);
    }

    // Flush immediately if batch reached
    if (attendanceQueue.length >= MAX_BATCH) {
      try {
        flushAttendanceQueue();
      } catch (err) {
        logError(
          "flushAttendanceQueue (immediate) error: " + (err?.message || err),
        );
      }
    }

    // ====== MQTT publish (keep existing logic) ======
    if (client.connected) {
      console.log("MQTT client is connected");

      const fullName = (info.personName || info.persionName || "").trim();
      const firstName = fullName ? fullName.split(/\s+/)[0] : "";
      const lastName = fullName ? fullName.split(/\s+/).slice(1).join(" ") : "";

      const inout = "";

      const payload = {
        UserID: userId,
        employee: {
          first_name: firstName,
          last_name: lastName,
          full_name: fullName,
        },
        LogTime: info.time || null,
        device: {
          id: info.facesluiceId || null,
          name: info.facesluiceName || null,
        },
        inout,
        gps_location: "",

        RecordID: String(info.RecordID || ""),
        is_live: String(info.PushType) === "0",
        message: "queued",
      };

      const topic = payload.is_live
        ? `mqtt/face/${info.facesluiceId}/recods/livelogs`
        : `mqtt/face/${info.facesluiceId}/recods/missinglogs`;

      client.publish(topic, JSON.stringify(payload), { qos: 1 });
      console.log("MQTT client published message", payload);
    } else {
      console.log("MQTT client is not connected");
      logError(
        "MQTT publish failed: client not connected. Payload: " +
        JSON.stringify(sanitizeInfo(info)),
      );
    }
  });
})(); // end async IIFE

// ========== GRACEFUL SHUTDOWN ==========
function shutdown() {
  console.log("Shutting down...");
  try {
    flushAttendanceQueue();
  } catch (err) {
    logError("Shutdown flush error: " + (err?.message || err));
  }
  try {
    if (aedes) aedes.close();
    if (brokerServer) brokerServer.close();
    console.log("Aedes broker closed.");
  } catch (err) {
    logError("Aedes close error: " + (err?.message || err));
  }
  try {
    db.close();
    console.log("SQLite database closed.");
  } catch (err) {
    logError("SQLite close error: " + (err?.message || err));
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
