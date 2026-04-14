const { Aedes } = require('aedes');
const net = require('net');
const http = require('http');
const websocketStream = require('websocket-stream');

const PORT = 1883;
const WS_PORT = 8083;

function forwardToAPI(path, data) {
  const postData = JSON.stringify(data);
  const req = http.request({
    hostname: 'localhost', port: 8000, path, method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(postData) },
  }, (res) => {
    console.log(`[MQTT->API] ${path} => HTTP ${res.statusCode}`);
  });
  req.on('error', (e) => console.error(`[MQTT->API] Error: ${e.message}`));
  req.write(postData);
  req.end();
}

(async () => {
  const aedes = await Aedes.createBroker();
  const server = net.createServer(aedes.handle);

  aedes.on('client', (client) => {
    console.log(`[MQTT] Client connected: ${client.id}`);
  });

  aedes.on('clientDisconnect', (client) => {
    console.log(`[MQTT] Client disconnected: ${client.id}`);
  });

  aedes.on('publish', (packet, client) => {
    if (!client) return;

    const topic = packet.topic;
    const payload = packet.payload.toString();

    try {
      const data = JSON.parse(payload);

      if (data.operator === 'RecPush') {
        console.log(`[MQTT] RecPush payload: ${payload.substring(0, 500)}`);
        forwardToAPI('/api/Subscribe/Verify', data);
      } else if (data.operator === 'VerifyPush' || topic.includes('Verify') || topic.includes('pass_record')) {
        console.log(`[MQTT] Face event: ${data.info?.Name || data.person_name || '?'} | ID: ${data.info?.PersonUUID || data.person_code || '?'} | Device: ${data.info?.DeviceID || data.device_sn || '?'}`);
        forwardToAPI('/api/Subscribe/Verify', data);
      } else if (data.json_flag === 'pass_record' || data.person_code) {
        console.log(`[MQTT] OX-900 event: ${data.person_name || '?'} | Code: ${data.person_code}`);
        forwardToAPI('/api/camera2_push_events', data);
      } else if (data.operator === 'HeartBeat') {
        // Silent
      } else {
        console.log(`[MQTT] ${data.operator || topic} | Device: ${data.info?.DeviceID || '?'}`);
      }
    } catch (e) {
      if (payload.length > 10) {
        console.log(`[MQTT] Raw: ${topic} | ${payload.substring(0, 100)}`);
      }
    }
  });

  server.listen(PORT, () => {
    console.log(`[MQTT] Broker running on port ${PORT}`);
    console.log(`[MQTT] Forwarding face events to Laravel API`);
  });

  const httpServer = http.createServer((req, res) => {
    if (req.method === 'POST' && req.url === '/publish') {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        try {
          const { topic, payload } = JSON.parse(body);
          aedes.publish({ topic, payload: typeof payload === 'string' ? payload : JSON.stringify(payload), qos: 0 });
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(404);
    res.end();
  });
  websocketStream.createServer({ server: httpServer }, aedes.handle);
  httpServer.listen(WS_PORT, () => {
    console.log(`[MQTT] WebSocket broker + HTTP relay running on port ${WS_PORT}`);
  });
})();
