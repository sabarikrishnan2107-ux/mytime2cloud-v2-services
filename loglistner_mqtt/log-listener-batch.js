const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");
require("dotenv").config();

// ========== CONFIG ==========
const { SOCKET_ENDPOINT } = process.env;

console.table({
  SOCKET_ENDPOINT,
  DB_HOST: process.env.DB_HOST,
  DB_PORT: process.env.DB_PORT,
  DB_USERNAME: process.env.DB_USERNAME,
  DB_PASSWORD: process.env.DB_PASSWORD ? "***hidden***" : "NOT SET",
  DB_DATABASE: process.env.DB_DATABASE,
  PGPOOL_MAX: process.env.PGPOOL_MAX || 20,
  DEVICE_CACHE_TTL: process.env.DEVICE_CACHE_TTL_MS || "30000 (default)",
  MYTIME_LOG_DIR: process.env.MYTIME_LOG_DIR || "logs/ (default)",
  MAX_QUEUE: 5000,
  MAX_BATCH: 300,
  FLUSH_MS: 300,
});

const verification_methods = {
  1: "Card", 2: "Fing", 3: "Face", 4: "Fing + Card",
  5: "Face + Fing", 6: "Face + Card", 7: "Card + Pin",
  8: "Face + Pin", 9: "Fing + Pin", 10: "Manual",
  11: "Fing + Card + Pin", 12: "Face + Card + Pin",
  13: "Face + Fing + Pin", 14: "Face + Fing + Card", 15: "Repeated",
};

const reasons = {
  16: "Date Expire", 17: "Timezone Expire", 18: "Holiday",
  19: "Unregistered", 20: "Detection lock", 23: "Loss Card",
  24: "Blacklisted", 25: "Without Verification", 26: "No Card Verification",
  27: "No Fingerprint",
};

// ========== ERROR LOGGING ==========
function nowGMT4() {
  const now = new Date();
  return new Date(now.toLocaleString("en-US", { timeZone: "Asia/Dubai" }));
}

function getErrorFile() {
  return path.join(__dirname, `error-${nowGMT4().toISOString().slice(0, 10)}.log`);
}

function logError(message) {
  const line = `[${nowGMT4().toISOString()}] ${message}\n`;
  try { fs.appendFileSync(getErrorFile(), line); } catch { }
  console.error("❌ ERROR:", message);
}

// ========== POSTGRESQL ==========
const dbPool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  max: process.env.PGPOOL_MAX ? Number(process.env.PGPOOL_MAX) : 20,
  idleTimeoutMillis: 0,
});

dbPool.connect()
  .then(c => { console.log("✅ PostgreSQL connected"); c.release(); })
  .catch(err => logError("PostgreSQL connection error: " + err.message));

// ========== DEVICE COMPANY CACHE ==========
// Registered devices → cached forever (until process restart)
// Unregistered devices → retried every DEVICE_CACHE_TTL_MS
const deviceCompanyCache = new Map();
const deviceCompanyCacheTS = new Map();
const PERMANENT_CACHE = new Set();
const DEVICE_CACHE_TTL_MS = process.env.DEVICE_CACHE_TTL_MS
  ? Number(process.env.DEVICE_CACHE_TTL_MS)
  : 30 * 1000; // 30 sec for unregistered/null devices

async function getCompanyIdForDevice(deviceId) {
  const key = String(deviceId || "").trim();
  if (!key) return null;

  // permanently cached registered device — never hit DB again
  if (PERMANENT_CACHE.has(key)) return deviceCompanyCache.get(key);

  // short TTL cache for unregistered/null devices
  const fresh = Date.now() - (deviceCompanyCacheTS.get(key) || 0) < DEVICE_CACHE_TTL_MS;
  if (fresh && deviceCompanyCache.has(key)) return deviceCompanyCache.get(key);

  try {
    const r = await dbPool.query(
      `SELECT company_id FROM devices WHERE serial_number = $1::text OR device_id = $1::text LIMIT 1`,
      [key]
    );

    // device not found in devices table
    if (r.rows.length === 0) {
      logError(`Device not registered in devices table → DeviceID: ${key}`);
      deviceCompanyCache.set(key, null);
      deviceCompanyCacheTS.set(key, Date.now());
      return null;
    }

    // device found but company_id is null
    if (r.rows[0].company_id === null) {
      logError(`Device found but company_id is NULL → DeviceID: ${key}`);
      deviceCompanyCache.set(key, null);
      deviceCompanyCacheTS.set(key, Date.now());
      return null;
    }

    // fully registered — cache forever
    const cid = r.rows[0].company_id;
    deviceCompanyCache.set(key, cid);
    PERMANENT_CACHE.add(key);
    console.log(`📌 Device permanently cached → DeviceID: ${key} | company_id: ${cid}`);
    return cid;

  } catch (err) {
    logError(`getCompanyIdForDevice failed (${key}): ${err.message}`);
    return null;
  }
}

// ========== BULK INSERT ==========
// ✅ Run once in PostgreSQL before starting:
//   CREATE UNIQUE INDEX IF NOT EXISTS attendance_logs_uniq
//   ON attendance_logs ("DeviceID", "LogTime", "UserID");

const attendanceQueue = [];
const MAX_QUEUE = 5000;
const MAX_BATCH = 300;
const FLUSH_MS = 300;

const logDir = process.env.MYTIME_LOG_DIR || path.join(__dirname, "logs");
if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

function buildAttendanceBulkInsert(rows) {
  const cols = 13;
  const params = [];
  const valuesSql = rows.map((r, i) => {
    params.push(
      r.UserID, r.DeviceID, r.company_id, r.LogTime, r.SerialNumber,
      r.status, r.mode, r.reason, r.log_date_time, r.index_serial_number,
      r.log_date, r.created_at, r.updated_at
    );
    const base = i * cols;
    return `(${Array.from({ length: cols }, (_, k) => `$${base + k + 1}`).join(",")})`;
  }).join(",");

  return {
    sql: `
      INSERT INTO attendance_logs (
        "UserID","DeviceID","company_id","LogTime","SerialNumber",
        "status","mode","reason","log_date_time","index_serial_number",
        "log_date","created_at","updated_at"
      ) VALUES ${valuesSql}
      ON CONFLICT ("DeviceID","LogTime","UserID") DO NOTHING
    `,
    params,
  };
}

async function flushAttendanceQueue() {
  if (attendanceQueue.length === 0) return;

  const batch = attendanceQueue.splice(0, MAX_BATCH);

  // log any null company_id rows before insert
  const nullCompanyRows = batch.filter(r => !r.company_id);
  if (nullCompanyRows.length > 0) {
    nullCompanyRows.forEach(r => {
      logError(`NULL company_id → UserID: ${r.UserID} | DeviceID: ${r.DeviceID} | LogTime: ${r.LogTime}`);
    });
  }

  try {
    const { sql, params } = buildAttendanceBulkInsert(batch);
    const result = await dbPool.query(sql, params);

    const inserted = result.rowCount;
    const skipped = batch.length - inserted;

    if (skipped > 0) {
      batch.forEach(r => {
        console.log(`⏭️  Skipped duplicate → UserID: ${r.UserID} | DeviceID: ${r.DeviceID} | LogTime: ${r.LogTime}`);
      });
    }

    console.log(`✅ Flushed ${batch.length} rows | inserted: ${inserted} | skipped: ${skipped}`);
  } catch (err) {
    logError("Bulk attendance insert failed: " + err.message);
    try {
      const fallbackFile = path.join(logDir, `failed-attendance-${nowGMT4().toISOString().slice(0, 10)}.jsonl`);
      fs.appendFileSync(fallbackFile, batch.map(b => JSON.stringify(b)).join("\n") + "\n");
    } catch (e) {
      logError("Failed to write fallback file: " + e.message);
    }
  }
}

// Periodic flush every 300ms
setInterval(() => {
  flushAttendanceQueue().catch(err => logError("flushAttendanceQueue error: " + err.message));
}, FLUSH_MS);

// ========== WEBSOCKET ==========
let socket;
const reconnectInterval = 5000;

function getFormattedDate() {
  const options = {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  };
  const [newDate, newTime] = new Intl.DateTimeFormat("en-US", options)
    .format(new Date()).split(",");
  const [m, d, y] = newDate.split("/");
  return { date: `${d.padStart(2, "0")}-${m.padStart(2, "0")}-${y}`, time: newTime };
}

function connectWebSocket() {
  socket = new WebSocket(SOCKET_ENDPOINT);

  socket.onopen = () => {
    const { date, time } = getFormattedDate();
    console.log(`📡 Connected to ${SOCKET_ENDPOINT} at ${date} ${time}`);
  };

  socket.onerror = (error) => {
    const { date, time } = getFormattedDate();
    console.error(`WebSocket error: ${error.message} at ${date} ${time}`);
  };

  socket.onclose = (event) => {
    const { date, time } = getFormattedDate();
    console.warn(`WebSocket closed (code ${event.code}) at ${date} ${time}`);
    setTimeout(connectWebSocket, reconnectInterval);
  };

  socket.onmessage = async ({ data }) => {
    try {
      const jsonData = JSON.parse(data).Data;
      const { UserCode, SN, RecordDate, RecordNumber, RecordCode } = jsonData;

      if (!UserCode || UserCode <= 0) return;


      // ── Build row ──
      const status = RecordCode > 15 ? "Access Denied" : "Allowed";
      const mode = verification_methods[RecordCode] ?? "---";
      const reason = reasons[RecordCode] ?? "---";
      // Normalize LogTime: ensure HH:MM:SS format (pad with :00 if missing seconds)
      let normalizedTime = String(RecordDate || "").trim();
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(normalizedTime)) {
        normalizedTime += ":00";
      }
      const logDate = normalizedTime.split(" ")[0];
      const companyId = await getCompanyIdForDevice(SN);

      console.log(`📥 Received: ${UserCode}_${SN}_${normalizedTime} for ${companyId}`);

      const row = {
        UserID: UserCode,
        DeviceID: String(SN),
        company_id: companyId,
        LogTime: normalizedTime,
        SerialNumber: RecordNumber || null,
        status,
        mode,
        reason,
        log_date_time: normalizedTime,
        index_serial_number: RecordNumber || null,
        log_date: logDate,
        created_at: nowGMT4(),
        updated_at: nowGMT4(),
      };

      // ── Queue with overflow protection ──
      if (attendanceQueue.length >= MAX_QUEUE) {
        try {
          const overflowFile = path.join(logDir, `overflow-attendance-${nowGMT4().toISOString().slice(0, 10)}.jsonl`);
          fs.appendFileSync(overflowFile, JSON.stringify(row) + "\n");
          logError(`Queue overflow (>${MAX_QUEUE}). Row written to overflow file.`);
        } catch (e) {
          logError("Queue overflow + failed to write overflow file: " + e.message);
        }
        return;
      }

      attendanceQueue.push(row);

      // Flush immediately if batch size reached
      if (attendanceQueue.length >= MAX_BATCH) {
        flushAttendanceQueue().catch(err => logError("flushAttendanceQueue (immediate): " + err.message));
      }
    } catch (error) {
      console.error("Error processing message:", error.message);
    }
  };

  process.on("SIGTERM", () => {
    const { date, time } = getFormattedDate();
    console.log(`Process killed at ${date} ${time}`);
    process.exit(0);
  });

  process.on("SIGINT", () => {
    const { date, time } = getFormattedDate();
    console.log(`Process killed at ${date} ${time}`);
    process.exit(0);
  });
}

connectWebSocket();