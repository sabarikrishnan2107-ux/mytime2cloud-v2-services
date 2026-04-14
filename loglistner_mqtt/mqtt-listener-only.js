require("dotenv").config();
const mqtt = require("mqtt");
const { WebSocketServer } = require("ws"); // Standard WebSocket library
const fs = require("fs");
const path = require("path");

// ... (Keep your todayGMT4 and sanitize functions) ...

// ========== CONFIG ==========
const MQTT_HOST = process.env.MQTT_HOST || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const WS_PORT = 4001; // The port your React app will connect to

// 1. Initialize WebSocket Server
// const wss = new WebSocketServer({ port: WS_PORT });
console.log(`🚀 WebSocket Server started on ws://localhost:${WS_PORT}`);

// 2. Initialize MQTT Client
const mqttClient = mqtt.connect(`${MQTT_HOST}:${MQTT_PORT}`, {
  clientId: `bridge_node_${Math.random().toString(36).substring(7)}`,
});

mqttClient.on("connect", () => {
  console.log(`📡 Connected to MQTT: ${MQTT_HOST}`);
  mqttClient.subscribe(["mqtt/face/+/+", "mqtt/face/heartbeat"]);
});

mqttClient.on("message", (topic, messageBuffer) => {
  let payload;
  try {
    payload = JSON.parse(messageBuffer.toString());
  } catch (e) { return; }

  const info = payload.info || {};

  // Handle Heartbeats (Log only)
  if (topic.includes("heartbeat")) {
    console.log(`[HB] Device: ${info.facesluiceId}`);
  } 
  // Handle Attendance (Log + Send to Frontend)
  else {
    const user = info.RFIDCard || info.personId || "Unknown";
    // console.log(`[ATT] User: ${user} | Sending to WS clients...`);

    // Prepare clean data for Frontend
    const uiData = JSON.stringify({
      id: user,
      name: info.personName || `User ${user}`,
      location: info.facesluiceName || info.facesluiceId,
      time: info.time || new Date().toISOString(),
      type: "ATTENDANCE"
    });

    // 3. BROADCAST to all connected React clients
    // wss.clients.forEach((client) => {
    //   if (client.readyState === 1) { // 1 = OPEN
    //     client.send(uiData);
    //   }
    // });
  }
});