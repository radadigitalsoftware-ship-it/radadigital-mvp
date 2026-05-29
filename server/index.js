'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const PORT = process.env.PORT || 3000;
const WS_PATH = '/ws/ais';
const WS_PING_INTERVAL_MS = 30000;
const API_KEY = process.env.AISSTREAM_API_KEY;
const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';
const AIS_BBOX = [[-35.0, -58.5], [-34.0, -57.5]];
const RESPONSE_TIMEOUT_MS = 10000;
const FILTER_MESSAGE_TYPES = [
  'PositionReport',
  'ShipStaticData',
  'ExtendedClassBPositionReport',
  'StandardClassBPositionReport'
];

const STATIC_ROOT = path.join(__dirname, '..');
const app = express();
app.set('trust proxy', true);
app.use(express.static(STATIC_ROOT));

const server = http.createServer(app);
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

const wss = new WebSocketServer({
  server,
  path: WS_PATH,
  perMessageDeflate: false
});

let wsPingInterval = null;

const browserClients = new Set();
const RECONNECT_MS = 5000;
let aisUpstream = null;
let aisConnecting = false;
let reconnectTimer = null;
let aisResponseTimeout = null;
let recibioMensajeAis = false;

function logClaveDetectada() {
  console.log('Clave detectada:', API_KEY ? 'SÍ' : 'NO');
}

function describirCodigoCierre(code) {
  const mapa = {
    1000: 'cierre normal',
    1001: 'el endpoint se va',
    1006: 'cierre anormal (sin frame de cierre)',
    1008: 'violación de política',
    1009: 'mensaje demasiado grande',
    1011: 'error interno del servidor remoto',
    1012: 'reinicio del servicio',
    1013: 'intento de reconexión',
    4000: 'cierre personalizado (posible API key inválida o suscripción rechazada)'
  };
  return mapa[code] || 'motivo no documentado';
}

function subscriptionPayload() {
  return JSON.stringify({
    APIKey: API_KEY,
    BoundingBoxes: [AIS_BBOX],
    FilterMessageTypes: FILTER_MESSAGE_TYPES
  });
}

function broadcastStatus(connected) {
  const msg = JSON.stringify({ type: 'proxy_status', connected });
  browserClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

function enlazarKeepaliveCliente(client) {
  client.isAlive = true;
  client.on('pong', () => {
    client.isAlive = true;
  });
}

function iniciarKeepaliveClientes() {
  if (wsPingInterval) return;
  wsPingInterval = setInterval(() => {
    browserClients.forEach((client) => {
      if (client.isAlive === false) {
        client.terminate();
        browserClients.delete(client);
        closeAisUpstreamIfIdle();
        return;
      }
      client.isAlive = false;
      if (client.readyState === WebSocket.OPEN) {
        try {
          client.ping();
        } catch (_) {
          browserClients.delete(client);
        }
      }
    });
  }, WS_PING_INTERVAL_MS);
}

function detenerKeepaliveClientes() {
  if (wsPingInterval) {
    clearInterval(wsPingInterval);
    wsPingInterval = null;
  }
}

function enlazarKeepaliveUpstream(ws) {
  if (ws._aisPingInterval) return;
  ws._aisPingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.ping();
      } catch (_) {
        clearInterval(ws._aisPingInterval);
        ws._aisPingInterval = null;
      }
    }
  }, WS_PING_INTERVAL_MS);
  ws.on('close', () => {
    if (ws._aisPingInterval) {
      clearInterval(ws._aisPingInterval);
      ws._aisPingInterval = null;
    }
  });
}

function broadcastAis(text) {
  if (typeof text !== 'string') return;
  browserClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(text);
  });
}

function simplificarMensajeAis(parsed) {
  if (!parsed || typeof parsed !== 'object') return null;
  if (parsed.error) return null;

  const meta = parsed.MetaData || parsed.Metadata || parsed.metadata || {};
  const msgType = parsed.MessageType || parsed.messageType;
  const messageBag = parsed.Message || parsed.message || {};
  const body = (msgType && messageBag[msgType]) || messageBag.PositionReport
    || messageBag.ShipStaticData || messageBag.StandardClassBPositionReport
    || messageBag.ExtendedClassBPositionReport || {};

  const mmsi = meta.MMSI ?? meta.mmsi ?? body.UserID ?? body.userId ?? body.mmsi;
  if (mmsi == null || mmsi === '') return null;

  const name = meta.VesselName ?? meta.vessel_name ?? meta.vesselName
    ?? meta.ShipName ?? meta.ship_name ?? meta.shipName ?? meta.name
    ?? body.VesselName ?? body.vessel_name ?? body.vesselName
    ?? body.ShipName ?? body.ship_name ?? null;

  const latitude = meta.latitude ?? meta.Latitude ?? body.Latitude ?? body.latitude ?? null;
  const longitude = meta.longitude ?? meta.Longitude ?? body.Longitude ?? body.longitude ?? null;
  const status = body.NavigationalStatus ?? body.navigational_status
    ?? meta.NavigationalStatus ?? meta.navigational_status ?? null;
  const sog = body.Sog ?? body.sog ?? meta.sog ?? null;
  const course = body.Cog ?? body.cog ?? meta.Cog ?? meta.cog ?? null;
  const heading = body.TrueHeading ?? body.true_heading ?? body.Heading ?? body.heading
    ?? meta.TrueHeading ?? meta.heading ?? course ?? null;

  if (latitude == null && longitude == null && !name) return null;

  return {
    mmsi: String(mmsi),
    name,
    vesselName: name,
    latitude,
    longitude,
    status,
    sog,
    course,
    heading,
    messageType: msgType || null
  };
}

function limpiarTimeoutRespuesta() {
  if (aisResponseTimeout) {
    clearTimeout(aisResponseTimeout);
    aisResponseTimeout = null;
  }
}

function razonCierreAString(reason) {
  if (Buffer.isBuffer(reason)) return reason.toString('utf8').trim() || '(vacía)';
  if (reason == null || reason === '') return '(sin razón)';
  return String(reason).trim();
}

function armarTimeoutRespuesta(ws) {
  limpiarTimeoutRespuesta();
  recibioMensajeAis = false;
  aisResponseTimeout = setTimeout(() => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (recibioMensajeAis) return;
    console.log('⏳ Sin respuesta de AISstream. Reintentando...');
    ws.close();
  }, RESPONSE_TIMEOUT_MS);
}

function scheduleUpstreamReconnect() {
  clearTimeout(reconnectTimer);
  if (browserClients.size === 0) return;
  console.log(`Reintento de conexión AISstream en ${RECONNECT_MS / 1000}s…`);
  reconnectTimer = setTimeout(connectAisUpstream, RECONNECT_MS);
}

function enlazarEventosAisstream(ws) {
  ws.on('open', () => {
    aisConnecting = false;
    aisUpstream = ws;
    enlazarKeepaliveUpstream(ws);
    console.log('🌐 Conexión física establecida con AISstream. Enviando suscripción...');

    try {
      const payload = subscriptionPayload();
      ws.send(payload);
      console.log('📤 Suscripción enviada (APIKey + bbox', JSON.stringify(AIS_BBOX), ')');
      armarTimeoutRespuesta(ws);
      broadcastStatus(true);
      console.log('✅ AISstream listo — esperando primer mensaje AIS…');
    } catch (err) {
      console.error('❌ ERROR AL ENVIAR SUSCRIPCIÓN AISSTREAM:', err.message);
      limpiarTimeoutRespuesta();
      ws.close();
    }
  });

  ws.on('message', (data) => {
    if (!recibioMensajeAis) {
      recibioMensajeAis = true;
      limpiarTimeoutRespuesta();
      console.log('📡 Primer mensaje recibido de AISstream');
    }

    try {
      const text = typeof data === 'string' ? data : data.toString('utf8');
      const parsed = JSON.parse(text);
      if (parsed.error) {
        console.warn('AISstream:', parsed.error);
      } else {
        const meta = parsed.MetaData || parsed.Metadata || parsed.metadata || {};
        const nombre = meta.VesselName ?? meta.vessel_name ?? meta.ShipName ?? meta.ship_name ?? '(sin nombre)';
        const mmsi = meta.MMSI ?? meta.mmsi;
        console.log('🚢 Detectado:', nombre, 'MMSI:', mmsi);
        const simplified = simplificarMensajeAis(parsed);
        if (simplified) {
          broadcastAis(JSON.stringify(simplified));
        }
      }
    } catch (_) {
      /* mensaje no JSON; no se reenvía al cliente */
    }
  });

  ws.on('error', (err) => {
    aisConnecting = false;
    limpiarTimeoutRespuesta();
    console.error('❌ Error en WebSocket: ' + err);
    broadcastStatus(false);
  });

  ws.on('close', (code, reason) => {
    aisConnecting = false;
    limpiarTimeoutRespuesta();
    if (aisUpstream === ws) aisUpstream = null;
    broadcastStatus(false);

    const razon = razonCierreAString(reason);
    console.log('🔌 Conexión cerrada. Código: ' + code + ' Razón: ' + razon);
    if (describirCodigoCierre(code) !== 'motivo no documentado') {
      console.log('   →', describirCodigoCierre(code));
    }

    scheduleUpstreamReconnect();
  });
}

function connectAisUpstream() {
  logClaveDetectada();

  if (!API_KEY) {
    console.error('❌ ERROR AL CONECTAR CON AISSTREAM: Falta AISSTREAM_API_KEY en server/.env');
    scheduleUpstreamReconnect();
    return;
  }
  if (browserClients.size === 0) return;
  if (aisUpstream?.readyState === WebSocket.OPEN) return;
  if (aisConnecting) return;

  aisConnecting = true;
  console.log('Intentando conectar a AISstream…');

  try {
    const ws = new WebSocket(AISSTREAM_URL, {
      perMessageDeflate: false,
      handshakeTimeout: 15000
    });
    enlazarEventosAisstream(ws);
  } catch (error) {
    aisConnecting = false;
    console.error('❌ ERROR AL CONECTAR CON AISSTREAM:', error.message);
    broadcastStatus(false);
    scheduleUpstreamReconnect();
  }
}

function closeAisUpstreamIfIdle() {
  clearTimeout(reconnectTimer);
  limpiarTimeoutRespuesta();
  if (browserClients.size > 0) return;
  if (aisUpstream) {
    aisUpstream.removeAllListeners();
    aisUpstream.close();
    aisUpstream = null;
  }
}

wss.on('connection', (client, req) => {
  browserClients.add(client);
  enlazarKeepaliveCliente(client);
  iniciarKeepaliveClientes();
  connectAisUpstream();

  const peer = req.socket?.remoteAddress || 'cliente';
  console.log(`Cliente radar conectado (${peer}). Activos: ${browserClients.size}`);

  if (aisUpstream?.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify({ type: 'proxy_status', connected: true }));
  }

  client.on('close', () => {
    browserClients.delete(client);
    console.log(`Cliente radar desconectado. Activos: ${browserClients.size}`);
    closeAisUpstreamIfIdle();
    if (browserClients.size === 0) detenerKeepaliveClientes();
  });

  client.on('error', (err) => {
    console.warn('Error WebSocket cliente radar:', err.message);
    browserClients.delete(client);
    closeAisUpstreamIfIdle();
    if (browserClients.size === 0) detenerKeepaliveClientes();
  });
});

const listenPort = Number(PORT) || 3000;
server.listen(listenPort, '0.0.0.0', () => {
  console.log(`RadaDigital escuchando en 0.0.0.0:${listenPort} (PORT=${process.env.PORT ?? 'default'})`);
  console.log(`Radar AIS (proxy): ws(s)://<host>${WS_PATH}`);
  logClaveDetectada();
  if (!API_KEY) {
    console.warn('Copiá server/.env.example a server/.env y definí AISSTREAM_API_KEY');
  }
});
