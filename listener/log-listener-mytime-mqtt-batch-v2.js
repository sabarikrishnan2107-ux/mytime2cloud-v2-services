/**
 * mqtt-loglistener.js
 *
 * Improvements vs your current version:
 * 1) No per-message duplicate SELECT. Uses INSERT ... ON CONFLICT (requires UNIQUE index).
 * 2) Batches attendance inserts (queue + flush every FLUSH_MS or when MAX_BATCH reached).
 * 3) Caches device->company_id to avoid per-row subquery.
 * 4) Throttles heartbeat DB updates (writes at most once per device per MIN_HB_DB_WRITE_MS).
 *
 * ✅ IMPORTANT (run once in PostgreSQL):
 *   CREATE UNIQUE INDEX IF NOT EXISTS attendance_logs_uniq ON attendance_logs ("DeviceID", "LogTime", "UserID");
 */

require("dotenv").config();
const mqtt = require("mqtt");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");

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
    console.error("❌ FAILED to write error log:", e.message);
  }
  console.error("❌ ERROR:", message);
}

// Helper: sanitize info object for logging (remove big pic field)
function sanitizeInfo(info) {
  if (!info) return info;
  const clone = { ...info };
  if (clone.pic) clone.pic = "[BINARY_PIC_OMITTED]";
  return clone;
}

const uniqueId =
  "mqtt-loglistner-mytim2cloud-" +
  Math.random().toString(36).substring(2, 10) +
  "-" +
  Date.now();

// ========== CONFIG ==========

// MQTT
const MQTT_HOST = process.env.MQTT_HOST || "";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
//const MQTT_USERNAME = process.env.MQTT_USERNAME || "";
//const MQTT_PASSWORD = process.env.MQTT_PASSWORD || "";

console.log(MQTT_HOST);


const MQTT_TOPIC_ATT = process.env.MYTIME_MQTT_TOPIC_ATT || "";
const MQTT_TOPIC_HEARTBEAT = process.env.MYTIME_MQTT_TOPIC_HEARTBEAT || "";

// Use model_number to identify product type
const HEARTBEAT_MODEL_NUMBER =
  process.env.MYTIME_HEARTBEAT_MODEL_NUMBER || "MYTIME1"; // must match devices.model_number exactly

// PostgreSQL
const dbPool = new Pool({
  host: process.env.DB_HOST || "127.0.0.1",
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 5432,
  user: process.env.DB_USERNAME || "postgres",
  password: process.env.DB_PASSWORD || "test123",
  database: process.env.DB_DATABASE || "mytime2cloud-v2",
  max: 10,
  idleTimeoutMillis: 0,
});

// CSV logs directory (keeping your dir logic)
const logDir =
  process.env.MYTIME_LOG_DIR ||
  path.join(__dirname, "../backend/storage/app/mqtt-mytime-logs");

// Ensure log dir exists
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

function getTodayFile() {
  const today = todayGMT4().toISOString().slice(0, 10);
  return path.join(logDir, `logs-${today}.csv`);
}

// Track last heartbeat info for devices (multi-device support)
const heartbeatMap = new Map(); // key: serial/device_id, value: { lastHeartbeatTs, lastHeartbeatTimeStr }

// Device company cache (serial_number/device_id -> company_id)
const deviceCompanyCache = new Map(); // key: deviceId(string) -> company_id(int|null)
const DEVICE_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
const deviceCompanyCacheTS = new Map(); // key -> lastFetchedTs

async function getCompanyIdForDevice(deviceId) {
  const key = String(deviceId || "").trim();
  if (!key) return null;

  const ts = deviceCompanyCacheTS.get(key) || 0;
  const fresh = Date.now() - ts < DEVICE_CACHE_TTL_MS;

  if (fresh && deviceCompanyCache.has(key)) {
    return deviceCompanyCache.get(key);
  }

  try {
    const r = await dbPool.query(
      `SELECT company_id FROM devices WHERE serial_number = $1::text OR device_id = $1::text LIMIT 1`,
      [key],
    );
    const cid = r.rows[0]?.company_id ?? null;
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

// ========== TEST DB CONNECTION ==========
dbPool
  .connect()
  .then((client) => {
    console.log("✅ PostgreSQL connected");
    client.release();
  })
  .catch((err) =>
    logError("PostgreSQL connection error: " + (err?.message || err)),
  );

// ========== MQTT CLIENT ==========
const client = mqtt.connect(`${MQTT_HOST}:${MQTT_PORT}`, {
  // username: MQTT_USERNAME || undefined,
  // password: MQTT_PASSWORD || undefined,
  clientId: `gateway-${uniqueId}`,
  keepalive: 30,
});

client.on("connect", () => {
  console.log("📡 MQTT connected to", MQTT_HOST);

  client.subscribe([MQTT_TOPIC_ATT, MQTT_TOPIC_HEARTBEAT], (err) => {
    if (err) {
      logError("Subscribe error: " + err.message);
    } else {
      console.log(
        "✅ Subscribed to:",
        MQTT_TOPIC_ATT,
        "and",
        MQTT_TOPIC_HEARTBEAT,
      );
    }
  });
});

client.on("error", (err) => logError("MQTT error: " + err.message));

// ========== HEARTBEAT STATUS CHECK (EVERY HOUR) ==========
async function verifyDevicesOnlineStatus() {
  const now = Date.now();
  const ONE_HOUR_MS = 60 * 60 * 1000;

  if (heartbeatMap.size === 0) {
    console.log(
      "⚠️ No heartbeat received for any device yet. Skipping status check.",
    );
    return;
  }

  for (const [serial, hbInfo] of heartbeatMap.entries()) {
    const { lastHeartbeatTs } = hbInfo || {};
    let sql, params;

    if (lastHeartbeatTs && now - lastHeartbeatTs <= ONE_HOUR_MS) {
      // ONLINE
      sql = `
        UPDATE devices 
        SET status_id = 1 
        WHERE model_number = $1 
          AND (serial_number = $2 OR device_id = $2)
      `;
      params = [HEARTBEAT_MODEL_NUMBER, serial];

      console.log(
        "✅ Heartbeat OK in last hour. Setting device ONLINE:",
        serial,
        "| last HB:",
        new Date(lastHeartbeatTs).toISOString(),
      );
    } else {
      // OFFLINE
      sql = `
        UPDATE devices 
        SET status_id = 2 
        WHERE model_number = $1 
          AND (serial_number = $2 OR device_id = $2)
      `;
      params = [HEARTBEAT_MODEL_NUMBER, serial];

      console.log(
        "⚠️ No heartbeat in last hour. Setting device OFFLINE:",
        serial,
        "| last HB:",
        lastHeartbeatTs ? new Date(lastHeartbeatTs).toISOString() : "never",
      );
    }

    try {
      await dbPool.query(sql, params);
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
  verifyDevicesOnlineStatus().catch((err) =>
    logError("verifyDevicesOnlineStatus error: " + (err?.message || err)),
  );
}, 60 * 60 * 1000);

// ========== HEARTBEAT DB WRITE THROTTLE ==========
const lastHeartbeatDbWriteMap = new Map(); // serial -> ts
const MIN_HB_DB_WRITE_MS = 60 * 1000; // write heartbeat to DB at most once per minute per device

// ========== ATTENDANCE BATCH INSERT ==========
const attendanceQueue = [];
const MAX_QUEUE = 5000; // safety: prevent unlimited memory growth
const MAX_BATCH = 300; // rows per flush
const FLUSH_MS = 300; // flush interval

// Attendance UPSERT/DO NOTHING requires unique index:
// ("DeviceID","LogTime","UserID")
function buildAttendanceBulkInsert(rows) {
  // Each row is an object with normalized fields we want to insert.
  // We'll insert with ON CONFLICT DO NOTHING.
  // Columns count = 13 (we include company_id now).
  const cols = 13;
  const params = [];
  const valuesSql = rows
    .map((r, i) => {
      // Order must match the VALUES aliases below.
      const rowValues = [
        r.UserID, //1
        r.DeviceID, //2
        r.company_id, //3
        r.LogTime, //4
        r.SerialNumber, //5
        r.status, //6
        r.mode, //7
        r.reason, //8
        r.log_date_time, //9
        r.index_serial_number, //10
        r.log_date, //11
        r.created_at, //12
        r.updated_at, //13
      ];
      params.push(...rowValues);
      const base = i * cols;
      const ph = Array.from({ length: cols }, (_, k) => `$${base + k + 1}`).join(
        ",",
      );
      return `(${ph})`;
    })
    .join(",");

  const sql = `
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
    VALUES ${valuesSql}
    ON CONFLICT ("DeviceID","LogTime","UserID") DO NOTHING
  `;

  return { sql, params };
}

async function flushAttendanceQueue() {
  if (attendanceQueue.length === 0) return;

  const batch = attendanceQueue.splice(0, MAX_BATCH);

  try {
    const { sql, params } = buildAttendanceBulkInsert(batch);
    let response = await dbPool.query(sql, params);

    console.log(`pg response:`);
    console.log(`${response}`);


  } catch (err) {
    logError("Bulk attendance insert failed: " + (err?.message || err));
    // Optional fallback: write batch to a file for later reprocessing
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
  flushAttendanceQueue().catch((err) =>
    logError("flushAttendanceQueue error: " + (err?.message || err)),
  );
}, FLUSH_MS);

// ========== MQTT MESSAGE HANDLER ==========
client.on("message", async (receivedTopic, messageBuffer) => {
  console.log(
    "-----------------------------------------------------------LOG Listener - receivedTopic",
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
    const serialNumber = hbInfo.facesluiceId || null; // device serial_number or device_id
    const hbTimeStr = hbInfo.time || null; // "2025-11-22 15:49:29"

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
      await dbPool.query(
        `UPDATE devices 
           SET status_id = 1,
               last_live_datetime = $3
         WHERE model_number = $1 
           AND (serial_number = $2 OR device_id = $2)`,
        [HEARTBEAT_MODEL_NUMBER, serialNumber, hbTimeStr],
      );

      lastHeartbeatDbWriteMap.set(serialNumber, Date.now());

      console.log(
        "✅ Device ONLINE & last_live_datetime updated (throttled) (model:",
        HEARTBEAT_MODEL_NUMBER,
        "key:",
        serialNumber + ")",
      );
    } catch (err) {
      logError(
        "Failed to update devices (heartbeat): " + (err?.message || err),
      );
    }

    return; // don't process heartbeat as attendance
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
    logError("Skipping insert: Missing facesluiceId (DeviceID). Payload: " + JSON.stringify(safeInfo));
    return;
  }

  // Company id (cached)
  const companyId = await getCompanyIdForDevice(deviceId);

  // Prepare row for batch insert
  const row = {
    UserID: userId,
    DeviceID: deviceId,
    company_id: companyId, // can be null
    LogTime: logTime,
    SerialNumber: info.RecordID || null,
    status: info.VerifyStatus == 1 ? "Allowed" : "Denied",
    mode: "Face",
    reason: "---",
    log_date_time: timeStr,
    index_serial_number: info.RecordID || null,
    log_date: logDate,
    created_at: todayGMT4(),
    updated_at: todayGMT4(),
  };

  // Queue safety (backpressure)
  if (attendanceQueue.length >= MAX_QUEUE) {
    // Drop oldest or write to file to avoid memory blow
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
    flushAttendanceQueue().catch((err) =>
      logError("flushAttendanceQueue (immediate) error: " + (err?.message || err)),
    );
  }

  // ====== MQTT publish (keep your logic) ======
  if (client.connected) {
    console.log("MQTT client is connected");

    const fullName = (info.personName || info.persionName || "").trim();
    const firstName = fullName ? fullName.split(/\s+/)[0] : "";
    const lastName = fullName ? fullName.split(/\s+/).slice(1).join(" ") : "";

    const inout = "";

    // Note: previously you computed message based on DB result.
    // Now inserts are batched async, so we publish "queued" immediately.
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
      message: "queued", // ✅ changed: DB happens in background batch
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
