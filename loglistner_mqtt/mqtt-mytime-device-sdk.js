// mqtt-gateway.js
// HTTP -> MQTT gateway for FRT/face devices
// Features:
// - 12 device operations (status, open door, time, person CRUD/list/search)
// - Env-based topic prefix (MQTT_TOPIC_PREFIX)
// - Error & activity logs (daily rotated files)
// - JSON logs (error/activity/events/route/basic-nonpending)
// - Log retention (LOG_RETENTION_DAYS)
// - CorrelationId per HTTP request
// - Device event logging
// - JSON log endpoints (no UI)
require("dotenv").config();
const mqtt = require("mqtt");
const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const os = require("os");
const http = require("http");

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

//const { v4: uuidv4 } = require("uuid");
const uniqueId =
  "mytim2cloud-" +
  Math.random().toString(36).substring(2, 10) +
  "-" +
  Date.now();
// const uuidv4 =
//   "mytim2cloud-" +
//   Math.random().toString(36).substring(2, 10) +
//   "-" +
//   Date.now();
const { AsyncLocalStorage } = require("async_hooks");

// ======= CONFIG =======


const MQTT_HOST = `mqtt://${localIp}`;
const MQTT_PORT = 1883;

// Topic prefix: e.g. "mqtt/face"
const MQTT_TOPIC_PREFIX = process.env.MQTT_TOPIC_PREFIX || "";

const HTTP_PORT = process.env.HTTP_PORT || 8001;

// Log directory + retention
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "logs");
const LOG_RETENTION_DAYS = parseInt(process.env.LOG_RETENTION_DAYS || "30", 10);

// Ensure log dir exists
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// ======= ASYNC CONTEXT (for correlationId) =======
const requestContext = new AsyncLocalStorage();

// ======= LOGGING HELPERS =======

function getTodayDateString() {
  return todayGMT4().toISOString().slice(0, 10); // YYYY-MM-DD
}

function todayGMT4() {
  const now = new Date();
  const gmt4 = new Date(
    now.toLocaleString("en-US", { timeZone: "Asia/Dubai" }),
  );
  return gmt4;
}

function getErrorLogFile() {
  return path.join(LOG_DIR, `mqtt-error-${getTodayDateString()}.log`);
}

function getActivityLogFile() {
  return path.join(LOG_DIR, `mqtt-activity-${getTodayDateString()}.log`);
}

function getErrorJsonLogFile() {
  return path.join(LOG_DIR, `mqtt-error-json-${getTodayDateString()}.log`);
}

function getActivityJsonLogFile() {
  return path.join(LOG_DIR, `mqtt-activity-json-${getTodayDateString()}.log`);
}

function getEventsJsonLogFile() {
  return path.join(LOG_DIR, `mqtt-events-json-${getTodayDateString()}.log`);
}

// NEW: dedicated log for HTTP routes (requests/responses)
function getRouteJsonLogFile() {
  return path.join(LOG_DIR, `mqtt-route-json-${getTodayDateString()}.log`);
}

// NEW: dedicated log for mqtt/face/basic MQTT IN (non-pending)
function getBasicNonPendingJsonLogFile() {
  return path.join(
    LOG_DIR,
    `mqtt-basic-nonpending-json-${getTodayDateString()}.log`,
  );
}

function safeAppend(filePath, content) {
  try {
    fs.appendFileSync(filePath, content);
  } catch (e) {
    console.error("❌ FAILED to write log:", e.message);
  }
}

function getCorrelationIdFromContext() {
  const store = requestContext.getStore();
  return store && store.correlationId ? store.correlationId : null;
}

function logError(message, details = {}) {
  const timestamp = todayGMT4().toISOString();
  const correlationId = getCorrelationIdFromContext();

  const blockLines = [];
  blockLines.push("==== ERROR =======================================");
  blockLines.push(`Time         : ${timestamp}`);
  if (correlationId) {
    blockLines.push(`CorrelationId: ${correlationId}`);
  }
  blockLines.push(`Message      : ${message}`);
  if (details && Object.keys(details).length > 0) {
    blockLines.push("Details      : " + JSON.stringify(details));
  }
  blockLines.push("=================================================");
  blockLines.push("");

  safeAppend(getErrorLogFile(), blockLines.join(os.EOL) + os.EOL);

  const jsonRecord = {
    level: "ERROR",
    timestamp,
    message,
    correlationId: correlationId || undefined,
    ...details,
  };

  safeAppend(getErrorJsonLogFile(), JSON.stringify(jsonRecord) + os.EOL);

  console.error("❌ ERROR:", message, details || "");
}

function logActivity(title, details = {}) {
  const timestamp = todayGMT4().toISOString();
  const correlationId = getCorrelationIdFromContext();

  const blockLines = [];
  blockLines.push("---- ACTIVITY ------------------------------------");
  blockLines.push(`Time         : ${timestamp}`);
  if (correlationId) {
    blockLines.push(`CorrelationId: ${correlationId}`);
  }
  blockLines.push(`Title        : ${title}`);
  if (details && Object.keys(details).length > 0) {
    blockLines.push("Details      : " + JSON.stringify(details));
  }
  blockLines.push("--------------------------------------------------");
  blockLines.push("");

  safeAppend(getActivityLogFile(), blockLines.join(os.EOL) + os.EOL);

  const jsonRecord = {
    level: "INFO",
    timestamp,
    title,
    correlationId: correlationId || undefined,
    ...details,
  };

  safeAppend(getActivityJsonLogFile(), JSON.stringify(jsonRecord) + os.EOL);

  console.log("📘 ACTIVITY:", title);
}

function logEvent(eventType, payload = {}, extra = {}) {
  const timestamp = todayGMT4().toISOString();
  const correlationId = getCorrelationIdFromContext();

  const jsonRecord = {
    level: "EVENT",
    timestamp,
    eventType,
    correlationId: correlationId || undefined,
    payload,
    ...extra,
  };

  safeAppend(getEventsJsonLogFile(), JSON.stringify(jsonRecord) + os.EOL);
}

// NEW: dedicated logger for HTTP routes
function logRoute(eventType, data = {}) {
  const timestamp = todayGMT4().toISOString();
  const correlationId = getCorrelationIdFromContext();

  const record = {
    timestamp,
    eventType,
    correlationId,
    ...data,
  };

  safeAppend(getRouteJsonLogFile(), JSON.stringify(record) + os.EOL);
}

// NEW: dedicated logger for mqtt/face/basic MQTT IN (non-pending)
function logBasicNonPending(data = {}) {
  const timestamp = todayGMT4().toISOString();

  const record = {
    timestamp,
    source: `${MQTT_TOPIC_PREFIX}/basic`,
    ...data,
  };

  safeAppend(getBasicNonPendingJsonLogFile(), JSON.stringify(record) + os.EOL);
}

// Cleanup old log files
function cleanupOldLogs() {
  try {
    const now = Date.now();
    const retentionMs = LOG_RETENTION_DAYS * 24 * 60 * 60 * 1000;

    const files = fs.readdirSync(LOG_DIR);
    files.forEach((file) => {
      if (!file.startsWith("mqtt-") || !file.endsWith(".log")) return;

      // Try to find YYYY-MM-DD in file name
      const match = file.match(/\d{4}-\d{2}-\d{2}/);
      if (!match) return;

      const dateStr = match[0];
      const fileTime = todayGMT4().getTime();
      if (isNaN(fileTime)) return;

      if (now - fileTime > retentionMs) {
        const fullPath = path.join(LOG_DIR, file);
        try {
          fs.unlinkSync(fullPath);
          console.log("🧹 Deleted old log:", fullPath);
        } catch (e) {
          console.error("❌ Failed to delete log", fullPath, e.message);
        }
      }
    });
  } catch (e) {
    console.error("❌ Error during log cleanup:", e.message);
  }
}

// Run cleanup at startup and once per day
cleanupOldLogs();
setInterval(cleanupOldLogs, 24 * 60 * 60 * 1000);

// ======= MQTT CLIENT =======
const client = mqtt.connect(`${MQTT_HOST}:${MQTT_PORT}`, {
  clientId: `gateway-${uniqueId}`,
  keepalive: 30,
});

// ======= DEVICE GATEWAY CLASS =======
class DeviceGateway {
  constructor(client) {
    this.client = client;

    // messageId -> {resolve, reject, timeout, expectedOperator}
    this.pendingRequests = new Map();

    // facesluiceId -> {online,lastOnline,lastOffline,lastHeartbeat}
    this.deviceStatus = {};

    // For EditPerson queue (must wait for Ack)
    this.personQueue = [];
    this.isProcessingPerson = false;

    this._setupListeners();
  }

  _setupListeners() {
    this.client.on("connect", () => {
      console.log("✅ MQTT connected");
      const subTopic = `${MQTT_TOPIC_PREFIX}/#`;
      this.client.subscribe(subTopic, (err) => {
        if (err) {
          logError("Failed to subscribe to MQTT topic", {
            topic: subTopic,
            error: err.message,
          });
        } else {
          console.log("🔎 Subscribed", subTopic);
        }
      });
    });

    this.client.on("error", (err) => {
      logError("MQTT error", { error: err.message });
    });

    this.client.on("message", (topic, messageBuf) => {
      let payload;
      try {
        payload = JSON.parse(messageBuf.toString());
        console.log("messageBuf", payload);
      } catch (e) {
        logError("Invalid JSON from MQTT", {
          topic,
          raw: messageBuf.toString().slice(0, 200),
          error: e.message,
        });
        return;
      }

      const { operator, messageId, info = {} } = payload;
      const facesluiceId = info.facesluiceId;
      const now = Date.now();

      // Determine deviceId & subTopicSuffix from topic based on prefix
      let deviceId = null;
      let subTopicSuffix = null;
      if (topic.startsWith(MQTT_TOPIC_PREFIX + "/")) {
        const rest = topic.slice(MQTT_TOPIC_PREFIX.length + 1); // after "mqtt/face/"
        const parts = rest.split("/");
        deviceId = parts[0] || null;
        subTopicSuffix = parts.slice(1).join("/") || "";
      }

      // ---- STATUS HANDLING (Online / Offline / Heartbeat) ----
      // Heartbeat over MQTT (if used)
      if (topic === `${MQTT_TOPIC_PREFIX}/heartbeat` && facesluiceId) {
        this.deviceStatus[facesluiceId] = {
          ...(this.deviceStatus[facesluiceId] || {}),
          lastHeartbeat: now,
        };
      }

      // Online/Offline on basic topic
      if (topic === `${MQTT_TOPIC_PREFIX}/basic` && facesluiceId) {
        if (operator === "Online") {
          this.deviceStatus[facesluiceId] = {
            ...(this.deviceStatus[facesluiceId] || {}),
            online: true,
            wifiIp: payload.wifiIp || null,

            lastOnline: now,
          };

          // Send Online-Ack to device
          const ackPayload = {
            operator: "Online-Ack",
            info: { facesluiceId },
          };
          const downTopic = `${MQTT_TOPIC_PREFIX}/${facesluiceId}`;
          this.client.publish(downTopic, JSON.stringify(ackPayload));

          logActivity("Device Online", {
            topic,
            deviceId: facesluiceId,
          });
        }

        if (operator === "Offline") {
          this.deviceStatus[facesluiceId] = {
            ...(this.deviceStatus[facesluiceId] || {}),
            online: false,
            lastOffline: now,
          };
          logActivity("Device Offline", {
            topic,
            deviceId: facesluiceId,
          });
        }
      }

      // ---- DEVICE EVENT LISTENER (QRCode / Card / Alarm etc.) ----
      const eventOperators = new Set([
        "QRCodePush",
        "CardPush",
        "AlarmInfoPush",
      ]);
      const isEvent =
        eventOperators.has(operator) ||
        subTopicSuffix === "QRCode" ||
        subTopicSuffix === "Card" ||
        subTopicSuffix === "Alarm";

      if (isEvent) {
        logEvent(operator || "DeviceEvent", payload, {
          topic,
          deviceId: facesluiceId || deviceId || null,
        });
      }

      // ---- ACK HANDLING (with echo-fix) ----
      if (messageId && this.pendingRequests.has(messageId)) {
        const entry = this.pendingRequests.get(messageId);
        const { expectedOperator, timeout } = entry;

        // Log inbound ack candidate (small summary only)
        logActivity("MQTT IN (Ack candidate)", {
          topic,
          operator,
          messageId,
          deviceId: facesluiceId || deviceId || null,
        });

        if (expectedOperator) {
          if (operator !== expectedOperator) {
            // Check if this is just an echo: operator === expectedOperator with "-Ack" removed
            const baseExpected = expectedOperator.endsWith("-Ack")
              ? expectedOperator.slice(0, -4)
              : expectedOperator;

            const isEcho = operator === baseExpected;

            if (isEcho) {
              // Ignore echo, wait for real Ack
              return;
            }

            // Not echo and not expected → error
            this.pendingRequests.delete(messageId);
            clearTimeout(timeout);
            const msg = `Unexpected operator for messageId ${messageId}. Expected ${expectedOperator}, got ${operator}`;
            logError(msg, { topic, operator, expectedOperator, messageId });
            entry.reject(new Error(msg));
            return;
          }
        }

        // Correct Ack received
        this.pendingRequests.delete(messageId);
        clearTimeout(timeout);
        entry.resolve(payload);
        return;
      }

      // ---- MQTT IN (non-pending) ----
      // If mqtt/face/basic → separate basic-nonpending log
      if (topic === `${MQTT_TOPIC_PREFIX}/basic`) {
        logBasicNonPending({
          topic,
          operator,
          messageId: messageId || null,
          deviceId: facesluiceId || deviceId || null,
          summary: "MQTT IN (non-pending) for basic",
        });
      } else {
        // Others go to normal activity log (summary only)
        logActivity("MQTT IN (non-pending)", {
          topic,
          operator,
          messageId: messageId || null,
          deviceId: facesluiceId || deviceId || null,
        });
      }
    });
  }

  // ======= CORE SEND =======
  sendCommand(deviceId, operator, info = {}, extra = {}, options = {}) {
    const messageId = extra.messageId || `ID:${uniqueId}`;
    const timeoutMs = options.timeoutMs || 15000;
    const expectedOperator = options.expectedAckOperator;

    const payload = {
      messageId,
      operator,
      info,
      ...extra,
    };

    const topicDown = `${MQTT_TOPIC_PREFIX}/${deviceId}`;

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(messageId);
        const err = new Error(
          `Timeout waiting for Ack for messageId ${messageId}`,
        );
        logError(err.message, {
          operator,
          deviceId,
          messageId,
        });
        reject(err);
      }, timeoutMs);

      this.pendingRequests.set(messageId, {
        resolve,
        reject,
        timeout,
        expectedOperator,
      });

      logActivity("MQTT OUT (command)", {
        topic: topicDown,
        operator,
        deviceId,
        messageId,
        info,
        extra,
      });

      console.log(`➡️ MQTT OUT to ${topicDown}:`, { payload });

      this.client.publish(
        topicDown,
        JSON.stringify(payload),
        { qos: 1 },
        (err) => {
          if (err) {
            clearTimeout(timeout);
            this.pendingRequests.delete(messageId);
            logError("MQTT publish failed", {
              topic: topicDown,
              operator,
              deviceId,
              messageId,
              error: err.message,
            });
            return reject(err);
          }
        },
      );
    });
  }

  // =========================================================
  // 1. Device Online Status
  // =========================================================
  getStatus(deviceId) {
    const s = this.deviceStatus[deviceId] || {};
    const now = Date.now();
    const ONLINE_TIMEOUT = 120000; // 2 minutes

    let online = !!s.online;
    if (s.lastHeartbeat && now - s.lastHeartbeat < ONLINE_TIMEOUT) {
      online = true;
    }

    return {
      deviceId,
      online,
      lastOnline: s.lastOnline || null,
      lastOffline: s.lastOffline || null,
      lastHeartbeat: s.lastHeartbeat || null,
    };
  }

  getMissingLogs(deviceId, date, serial_number) {
    return this.sendCommand(
      deviceId,
      "ManualPushRecords",
      {
        facesluiceId: deviceId,
        TimeS: `${date}T00:00:00`,
        TimeE: `${date}T23:59:59`,
      },
      {},
      { expectedAckOperator: "ManualPushRecords-Ack", timeoutMs: 15000 },
    );
  }
  GetDeviceInfo(deviceId, date, serial_number) {
    return this.sendCommand(
      deviceId,
      "GetNetConfig",
      {},
      {},
      { expectedAckOperator: "GetNetConfig-Ack", timeoutMs: 15000 },
    );
  }

  getBasic(deviceId, date, serial_number) {
    return this.sendCommand(
      deviceId,
      "Basic",
      {
        facesluiceId: deviceId,
      },
      {},
      { expectedAckOperator: "Basic-Ack", timeoutMs: 15000 },
    );
  }

  // =========================================================
  // 2. Door Open (Unlock / Unlock-Ack)
  // =========================================================
  async openDoor(deviceId) {
    return this.sendCommand(
      deviceId,
      "Unlock",
      {
        facesluiceId: deviceId,
        openDoor: 1,
        showInfo: "Door Open",
      },
      {},
      { expectedAckOperator: "Unlock-Ack", timeoutMs: 15000 },
    );
  }

  // =========================================================
  // 3. Door Close (NOT SUPPORTED by protocol)
  // =========================================================
  async closeDoor(deviceId) {
    return this.sendCommand(
      deviceId,
      "Unlock",
      {
        facesluiceId: deviceId,
        openDoor: 0,
        showInfo: "Door Closed",
      },
      {},
      { expectedAckOperator: "Unlock-Ack", timeoutMs: 15000 },
    );
  }

  // =========================================================
  // 6. Get / Set Time (GetSysTime / SetSysTime)
  // =========================================================
  async getTime(deviceId) {
    return this.sendCommand(
      deviceId,
      "GetSysTime",
      { facesluiceId: deviceId },
      {},
      { expectedAckOperator: "GetSysTime-Ack", timeoutMs: 15000 },
    );
  }

  async setTime(deviceId, sysTime) {
    console.log("Setting time:", { facesluiceId: deviceId, SysTime: sysTime });
    // sysTime example: "2025-11-22T18:30:00"
    return this.sendCommand(
      deviceId,
      "SetSysTime",
      { facesluiceId: deviceId, SysTime: sysTime },
      {},
      { expectedAckOperator: "SetSysTime-Ack", timeoutMs: 15000 },
    );
  }

  // =========================================================
  // 7 & 8. Add / Edit Person (EditPerson / EditPerson-Ack) with QUEUE
  // =========================================================
  async savePerson(deviceId, personInfo) {
    // Add request to queue to respect "wait for last Ack" rule
    return new Promise((resolve, reject) => {
      this.personQueue.push({ deviceId, personInfo, resolve, reject });
      this._processPersonQueue();
    });
  }

  async _sendEditPerson(deviceId, personInfo) {
    return this.sendCommand(
      deviceId,
      "EditPerson",
      personInfo,
      {},
      { expectedAckOperator: "EditPerson-Ack", timeoutMs: 30000 },
    );
  }

  async _processPersonQueue() {
    if (this.isProcessingPerson) return;
    if (this.personQueue.length === 0) return;

    this.isProcessingPerson = true;
    const { deviceId, personInfo, resolve, reject } = this.personQueue.shift();

    try {
      const result = await this._sendEditPerson(deviceId, personInfo);
      resolve(result);
    } catch (err) {
      logError("EditPerson queue error", {
        deviceId,
        error: err.message,
      });
      reject(err);
    } finally {
      this.isProcessingPerson = false;
      this._processPersonQueue();
    }
  }

  // =========================================================
  // 9. Batch Add / Edit Persons (EditPersonsNew / EditPersonsNew-Ack)
  // =========================================================
  async batchSavePersons(deviceId, personsArray) {
    return this.sendCommand(
      deviceId,
      "EditPersonsNew",
      {},
      {
        messageId: `EditPersonsNew-${Date.now()}`,
        DataBegin: "BeginFlag",
        PersonNum: personsArray.length,
        info: personsArray,
        DataEnd: "EndFlag",
      },
      { expectedAckOperator: "EditPersonsNew-Ack", timeoutMs: 60000 },
    );
  }

  // =========================================================
  // 10. Delete Person single / batch
  //    - DelPerson / DelPerson-Ack
  //    - DeletePersons / DeletePersons-Ack
  // =========================================================
  async deletePerson(deviceId, customId) {
    return this.sendCommand(
      deviceId,
      "DelPerson",
      { customId },
      {},
      { expectedAckOperator: "DelPerson-Ack", timeoutMs: 15000 },
    );
  }

  async batchDeletePersons(deviceId, customIds) {
    return this.sendCommand(
      deviceId,
      "DeletePersons",
      { customId: customIds },
      {
        messageId: `DeletePersons-${Date.now()}`,
        DataBegin: "BeginFlag",
        PersonNum: customIds.length,
        DataEnd: "EndFlag",
      },
      { expectedAckOperator: "DeletePersons-Ack", timeoutMs: 30000 },
    );
  }

  // =========================================================
  // 11. Search Person (SearchPerson / SearchPerson-Ack)
  // =========================================================
  async getPerson(deviceId, customId, includePicture = false) {
    return this.sendCommand(
      deviceId,
      "SearchPerson",
      { customId, Picture: includePicture ? 1 : 0 },
      {},
      { expectedAckOperator: "SearchPerson-Ack", timeoutMs: 15000 },
    );
  }

  // =========================================================
  // 12. Get All Persons List (SearchPersonList / SearchPersonList-Ack)
  // =========================================================
  async getAllPersons(deviceId) {
    return this.sendCommand(
      deviceId,
      "SearchPersonList",
      {
        facesluiceId: deviceId,
        personType: 2, // ALL persons
        BeginNO: 0,
        RequestCount: 2000,
      },
      {},
      { expectedAckOperator: "SearchPersonList-Ack", timeoutMs: 30000 },
    );
  }

  // Optional generic search with filters
  async searchPersonList(deviceId, params) {
    return this.sendCommand(
      deviceId,
      "SearchPersonList",
      params,
      {},
      { expectedAckOperator: "SearchPersonList-Ack", timeoutMs: 30000 },
    );
  }
}

// ======= SINGLETON GATEWAY =======
const gateway = new DeviceGateway(client);

// ======= EXPRESS API =======
const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

// Do NOT log route for log endpoints
const IGNORE_LOG_ROUTES = new Set([
  "/api/logs/error/today",
  "/api/logs/activity/today",
  "/api/logs/events/today",
]);

// CorrelationId + ROUTE logging middleware
app.use((req, res, next) => {
  const correlationId = req.headers["x-correlation-id"] || uniqueId;

  res.setHeader("X-Correlation-Id", correlationId);

  requestContext.run({ correlationId }, () => {
    if (!IGNORE_LOG_ROUTES.has(req.path)) {
      logRoute("HTTP_REQUEST", {
        method: req.method,
        path: req.path,
        query: req.query,
        body: req.method === "GET" ? undefined : req.body,
      });
    }
    next();
  });
});

const asyncHandler = (fn) => (req, res) =>
  Promise.resolve(fn(req, res)).catch((err) => {
    logError("Route error", {
      route: req.path,
      method: req.method,
      error: err.message,
      stack: err.stack,
    });
    res.status(500).json({ error: err.message || "Internal error" });
  });

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// 1. Missing Logs (simulate device retrying to send logs when it comes back online)

app.post(
  "/api/device/:deviceId/missinglogs",
  asyncHandler(async (req, res) => {
    console.log("missinglogs Request Received");

    const { deviceId } = req.params;
    const { date, serial_number } = req.body;
    const result = await gateway.getMissingLogs(deviceId, date, serial_number);
    logRoute("HTTP_RESPONSE", {
      route: "missinglogs",
      deviceId,
      date,
      serial_number,
      result,
    });
    res.json(result);
  }),
);

// 1. basic
app.get(
  "/api/device/:deviceId/basic",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = gateway.getBasic(deviceId);
    logRoute("HTTP_RESPONSE", { route: "basic", deviceId, result });
    res.json(result);
  }),
);
app.get(
  "/api/device/:deviceId/GetDeviceInfo",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = gateway.GetDeviceInfo(deviceId);
    logRoute("HTTP_RESPONSE", { route: "GetDeviceInfo", deviceId, result });
    res.json(result);
  }),
);

// 1. Status
app.get(
  "/api/device/:deviceId/status",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = gateway.getStatus(deviceId);
    logRoute("HTTP_RESPONSE", { route: "status", deviceId, result });
    res.json(result);
  }),
);

// 2. Open door
app.post(
  "/api/device/:deviceId/open-door",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = await gateway.openDoor(deviceId);
    logRoute("HTTP_RESPONSE", { route: "open-door", deviceId, result });
    res.json(result);
  }),
);

// 3. Close door (info only)
app.post(
  "/api/device/:deviceId/close-door",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = await gateway.closeDoor(deviceId);
    logRoute("HTTP_RESPONSE", { route: "close-door", deviceId, result });
    res.json(result);
  }),
);

// 6. Time get/set
app.get(
  "/api/device/:deviceId/timezone",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = await gateway.getTime(deviceId);
    logRoute("HTTP_RESPONSE", { route: "get-time", deviceId, result });
    res.json(result);
  }),
);

app.post(
  "/api/device/:deviceId/timezone",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const { sysTime } = req.body;
    const result = await gateway.setTime(deviceId, sysTime);
    logRoute("HTTP_RESPONSE", {
      route: "set-time",
      deviceId,
      sysTime,
      result,
    });
    res.json(result);
  }),
);

// 7 & 8. Add/Edit person
app.post(
  "/api/device/:deviceId/person",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const body = req.body;
    const result = await gateway.savePerson(deviceId, body);
    logRoute("HTTP_RESPONSE", {
      route: "save-person",
      deviceId,
      body,
      result,
    });
    res.json(result);
  }),
);

// 9. Batch add persons
app.post(
  "/api/device/:deviceId/persons/batch",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const persons = req.body.persons || [];
    const result = await gateway.batchSavePersons(deviceId, persons);
    logRoute("HTTP_RESPONSE", {
      route: "batch-save-persons",
      deviceId,
      count: persons.length,
      result,
    });
    res.json(result);
  }),
);

// 10. Delete single person
app.delete(
  "/api/device/:deviceId/person/:customId",
  asyncHandler(async (req, res) => {
    const { deviceId, customId } = req.params;
    const result = await gateway.deletePerson(deviceId, customId);
    logRoute("HTTP_RESPONSE", {
      route: "delete-person",
      deviceId,
      customId,
      result,
    });
    res.json(result);
  }),
);

// 10. Batch delete persons
app.post(
  "/api/device/:deviceId/persons/batch-delete",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const { customIds } = req.body;
    const result = await gateway.batchDeletePersons(deviceId, customIds || []);
    logRoute("HTTP_RESPONSE", {
      route: "batch-delete-persons",
      deviceId,
      count: (customIds || []).length,
      result,
    });
    res.json(result);
  }),
);

// 11. Search person (by customId)
app.get(
  "/api/device/:deviceId/person/:customId",
  asyncHandler(async (req, res) => {
    const { deviceId, customId } = req.params;
    const includePicture = req.query.picture === "1";
    const result = await gateway.getPerson(deviceId, customId, includePicture);
    logRoute("HTTP_RESPONSE", {
      route: "get-person",
      deviceId,
      customId,
      includePicture,
      result,
    });
    res.json(result);
  }),
);

// 12. Get all persons list
app.get(
  "/api/device/:deviceId/persons/list",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const result = await gateway.getAllPersons(deviceId);
    logRoute("HTTP_RESPONSE", {
      route: "list-persons",
      deviceId,
      resultSummary: {
        code: result.code,
        operator: result.operator,
      },
    });
    res.json(result);
  }),
);

// Optional: search list with filters
app.post(
  "/api/device/:deviceId/persons/search",
  asyncHandler(async (req, res) => {
    const { deviceId } = req.params;
    const payload = req.body || {};
    const result = await gateway.searchPersonList(deviceId, payload);
    logRoute("HTTP_RESPONSE", {
      route: "search-persons",
      deviceId,
      payload,
      resultSummary: {
        code: result.code,
        operator: result.operator,
      },
    });
    res.json(result);
  }),
);

// ===== LOG VIEW ENDPOINTS (JSON only) =====

function readTodayLogFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return { exists: false, content: "" };
  }
  try {
    const content = fs.readFileSync(filePath, "utf8");
    return { exists: true, content };
  } catch (e) {
    logError("Failed to read log file", {
      filePath,
      error: e.message,
    });
    return { exists: true, content: "" };
  }
}

// View today's error log (text)
app.get(
  "/api/logs/error/today",
  asyncHandler(async (req, res) => {
    const filePath = getErrorLogFile();
    const { exists, content } = readTodayLogFile(filePath);
    res.json({
      date: getTodayDateString(),
      file: path.basename(filePath),
      exists,
      content,
    });
  }),
);

// View today's activity log (text)
app.get(
  "/api/logs/activity/today",
  asyncHandler(async (req, res) => {
    const filePath = getActivityLogFile();
    const { exists, content } = readTodayLogFile(filePath);
    res.json({
      date: getTodayDateString(),
      file: path.basename(filePath),
      exists,
      content,
    });
  }),
);

// View today's events log (JSON lines)
app.get(
  "/api/logs/events/today",
  asyncHandler(async (req, res) => {
    const filePath = getEventsJsonLogFile();
    const { exists, content } = readTodayLogFile(filePath);
    res.json({
      date: getTodayDateString(),
      file: path.basename(filePath),
      exists,
      content,
    });
  }),
);

// Start server
const httpServer = http.createServer(app);

httpServer.listen(HTTP_PORT, () => {
  console.log(
    `🚀 MQTT data WebSocket Data gateway listening on http://localhost:${HTTP_PORT}`,
  );
});

module.exports = { gateway, DeviceGateway };
