const { Client } = require("pg");
const fs = require("fs");
const path = require("path");
require("dotenv").config();
const PQueue = require("p-queue").default; // Add .default here

const pushQueue = new PQueue({
  concurrency: 10, // Max 10 parallel pushes
  interval: 1000,  // Time window
  intervalCap: 100 // Max 100 per time window
});

const ignoredDevices = ["MYTIME1"];

// ========== ERROR LOGGING ==========
function nowGMT4() {
  const now = new Date();
  return new Date(now.toLocaleString("en-US", { timeZone: "Asia/Dubai" }));
}

function logError(message) {
  const file = path.join(__dirname, `error-${nowGMT4().toISOString().slice(0, 10)}.log`);
  const line = `[${nowGMT4().toISOString()}] ${message}\n`;
  try { fs.appendFileSync(file, line); } catch { }
  console.error("❌ ERROR:", message);
}

// ========== CONFIG ==========
const PUSH_URL = process.env.PUSH_URL || "https://push.mytime2cloud.com/notify";
const PUSH_SECRET = process.env.PUSH_SECRET || "";

console.table({
  DB_HOST: process.env.DB_HOST,
  DB_PORT: process.env.DB_PORT,
  DB_USERNAME: process.env.DB_USERNAME,
  DB_PASSWORD: process.env.DB_PASSWORD ? "***hidden***" : "NOT SET",
  DB_DATABASE: process.env.DB_DATABASE,
  PUSH_URL,
  PUSH_SECRET: PUSH_SECRET ? "***hidden***" : "NOT SET",
});

// ========== PUSH NOTIFICATION ==========
async function sendPushNotification(row) {
  try {
    const payload = {
      clientId: `${row.company_id}_${row.UserID}`,
      // clientId: `${row.company_id}`,
      type: "clock",
      message: "mobile clock in/out",
      data: {
        user_id: row.UserID,
        name: "",
        avatar: "",
        timestamp: row.LogTime,
        log_type: row.log_type,
      },
    };

    console.log(`📤 Sending push:`);
    console.log(JSON.stringify(payload, null, 2));

    const response = await fetch(PUSH_URL, {
      method: "POST",
      signal: AbortSignal.timeout(5000), // Stop waiting after 5 seconds
      headers: {
        "Content-Type": "application/json",
        ...(PUSH_SECRET ? { "Authorization": `Bearer ${PUSH_SECRET}` } : {}),
      },
      body: JSON.stringify(payload),
    });

    const responseText = await response.text();

    if (!response.ok) {
      logError(`Push failed (${response.status}): ${responseText}`);
      return;
    }

    console.log(`🔔 Push sent → UserID: ${row.UserID} | Device: ${row.DeviceID} | Status: ${row.status}`);
    console.log(`📬 Push server response: ${responseText}`);

  } catch (err) {
    logError(`sendPushNotification failed: ${err.message}`);
  }
}

// ========== PG NOTIFY LISTENER ==========
async function start() {
  const pgClient = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
  });

  try {
    await pgClient.connect();
    console.log("✅ PostgreSQL NOTIFY listener connected");

    await pgClient.query("LISTEN attendance_insert");
    console.log("👂 Listening for attendance_insert notifications...");

    pgClient.on("notification", async (msg) => {
      console.log(msg);

      // try {
      //   const row = JSON.parse(msg.payload);
      //   if (!ignoredDevices.some(id => row.DeviceID.includes(id))) {
      //     await sendPushNotification(row);
      //   } else {
      //     console.log(`⚠️ Ignored notification from DeviceID: ${JSON.stringify(row, null, 2)}`);
      //   }
      // } catch (err) {
      //   logError(`Notification parse failed: ${err.message}`);
      // }

      try {
        const row = JSON.parse(msg.payload);
        if (!ignoredDevices.some(id => row.DeviceID.includes(id))) {
          // Add to queue (non-blocking)
          pushQueue.add(async () => {
            console.log(`[Queue] Starting notification for Device: ${row.DeviceID}`);
            await sendPushNotification(row);
          }).then(() => {
            console.log(`[Queue] Successfully processed: ${row.DeviceID}`);
          }).catch((err) => {
            console.error(`[Queue] Failed for ${row.DeviceID}: ${err.message}`);
          });

          // This line runs immediately after adding to the queue
          console.log(`[Queue] Task buffered. Current size: ${pushQueue.size}`);
        }
      } catch (err) {
        logError(`Notification parse failed: ${err.message}`);
      }
    });

    pgClient.on("error", async (err) => {
      logError(`PG client error: ${err.message}`);
      try { await pgClient.end(); } catch { }
      console.log("🔄 Reconnecting in 5 seconds...");
      setTimeout(start, 5000);
    });

  } catch (err) {
    logError(`PG connect failed: ${err.message}`);
    console.log("🔄 Retrying in 5 seconds...");
    setTimeout(start, 5000);
  }
}

start();

pushQueue.on('idle', () => {
  console.log(`✅ Queue is empty. All notifications have been sent.`);
});

pushQueue.on('error', error => {
  logError(`Queue error: ${error.message}`);
});

// ========== GRACEFUL SHUTDOWN ==========
process.on("SIGTERM", () => {
  console.log("Process killed (SIGTERM)");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("Process killed (SIGINT)");
  process.exit(0);
});