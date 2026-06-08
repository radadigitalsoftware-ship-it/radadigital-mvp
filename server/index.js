'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
// Función dinámica para usar fetch en entorno CommonJS
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

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

// --- NUEVA LÓGICA DE INYECCIÓN VESSELFINDER ---
async function inyectarBarcosVesselFinder() {
    const url = 'https://www.vesselfinder.com/api/expl/as/list?v1';
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
                'Referer': 'https://www.vesselfinder.com/'
            }
        });
        if (!response.ok) return;
        const barcos = await response.json();
        
        barcos.forEach(b => {
            const barcoSimplificado = {
                mmsi: String(b.mmsi || b.imo || '0'),
                name: b.name || 'Barco VF',
                latitude: b.lat,
                longitude: b.lon,
                course: b.cog,
                sog: b.sog,
                source: 'vesselfinder'
            };
            broadcastAis(JSON.stringify(barcoSimplificado));
        });
        console.log(`✅ ${barcos.length} barcos inyectados desde VesselFinder.`);
    } catch (err) {
        console.error('❌ Error VesselFinder:', err.message);
    }
}

// Funciones originales de tu sistema...
function logClaveDetectada() { console.log('Clave detectada:', API_KEY ? 'SÍ' : 'NO'); }
function describirCodigoCierre(code) { const mapa = { 1000: 'cierre normal', 1006: 'cierre anormal', 4000: 'cierre personalizado' }; return mapa[code] || 'motivo no documentado'; }
function subscriptionPayload() { return JSON.stringify({ APIKey: API_KEY, BoundingBoxes: [AIS_BBOX], FilterMessageTypes: FILTER_MESSAGE_TYPES }); }
function broadcastStatus(connected) { const msg = JSON.stringify({ type: 'proxy_status', connected }); browserClients.forEach((c) => { if (c.readyState === WebSocket.OPEN) c.send(msg); }); }
function enlazarKeepaliveCliente(client) { client.isAlive = true; client.on('pong', () => { client.isAlive = true; }); }
function iniciarKeepaliveClientes() { if (wsPingInterval) return; wsPingInterval = setInterval(() => { browserClients.forEach((c) => { if (c.isAlive === false) { c.terminate(); browserClients.delete(c); closeAisUpstreamIfIdle(); return; } c.isAlive = false; if (c.readyState === WebSocket.OPEN) c.ping(); }); }, WS_PING_INTERVAL_MS); }
function detenerKeepaliveClientes() { if (wsPingInterval) { clearInterval(wsPingInterval); wsPingInterval = null; } }
function enlazarKeepaliveUpstream(ws) { if (ws._aisPingInterval) return; ws._aisPingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.ping(); }, WS_PING_INTERVAL_MS); ws.on('close', () => { clearInterval(ws._aisPingInterval); }); }
function broadcastAis(text) { if (typeof text !== 'string') return; browserClients.forEach((c) => { if (c.readyState === WebSocket.OPEN) c.send(text); }); }
function simplificarMensajeAis(parsed) { /* Tu lógica original de simplificación */ return parsed; } // Asegúrate de mantener tu lógica original aquí
function limpiarTimeoutRespuesta() { if (aisResponseTimeout) { clearTimeout(aisResponseTimeout); aisResponseTimeout = null; } }
function razonCierreAString(reason) { return reason ? String(reason) : '(sin razón)'; }
function armarTimeoutRespuesta(ws) { limpiarTimeoutRespuesta(); recibioMensajeAis = false; aisResponseTimeout = setTimeout(() => { if (ws.readyState === WebSocket.OPEN && !recibioMensajeAis) ws.close(); }, RESPONSE_TIMEOUT_MS); }
function scheduleUpstreamReconnect() { clearTimeout(reconnectTimer); if (browserClients.size === 0) return; reconnectTimer = setTimeout(connectAisUpstream, RECONNECT_MS); }

function enlazarEventosAisstream(ws) {
  ws.on('open', () => { aisConnecting = false; aisUpstream = ws; enlazarKeepaliveUpstream(ws); ws.send(subscriptionPayload()); broadcastStatus(true); });
  ws.on('message', (data) => { try { const text = data.toString(); const parsed = JSON.parse(text); if (parsed.MetaData) broadcastAis(text); } catch(e) {} });
  ws.on('close', () => { broadcastStatus(false); scheduleUpstreamReconnect(); });
}

function connectAisUpstream() {
  if (!API_KEY || browserClients.size === 0 || aisUpstream?.readyState === WebSocket.OPEN || aisConnecting) return;
  aisConnecting = true;
  try { const ws = new WebSocket(AISSTREAM_URL, { perMessageDeflate: false }); enlazarEventosAisstream(ws); } catch (e) { aisConnecting = false; }
}

function closeAisUpstreamIfIdle() { if (browserClients.size > 0) return; if (aisUpstream) { aisUpstream.close(); aisUpstream = null; } }

wss.on('connection', (client, req) => {
  browserClients.add(client);
  enlazarKeepaliveCliente(client);
  iniciarKeepaliveClientes();
  connectAisUpstream();
  client.on('close', () => { browserClients.delete(client); closeAisUpstreamIfIdle(); });
});

server.listen(listenPort, '0.0.0.0', () => {
    console.log(`RadaDigital activo en puerto ${PORT}`);
    // Inyectar VesselFinder cada 5 minutos
    setInterval(inyectarBarcosVesselFinder, 300000);
    inyectarBarcosVesselFinder();
});