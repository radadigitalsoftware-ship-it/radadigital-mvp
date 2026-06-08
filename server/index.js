'use strict';
const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

// Guardar el puerto de Azure/plataforma ANTES de cargar .env (evita pisar PORT con valores locales)
const platformPort = process.env.PORT;

require('dotenv').config({ path: path.join(__dirname, '.env') });

const PORT = platformPort || process.env.PORT || 3000;
const app = express();
app.use(express.static(path.join(__dirname, '..')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws/ais' });

const browserClients = new Set();

async function inyectarBarcosVesselFinder() {
    console.log('Sincronizando VesselFinder...');
    try {
        const response = await fetch('https://www.vesselfinder.com/api/expl/as/list?v1', {
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });

        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const barcos = await response.json();

        barcos.forEach(b => {
            broadcastAis(JSON.stringify({
                mmsi: String(b.mmsi || '0'),
                name: b.name || 'Barco',
                latitude: b.lat,
                longitude: b.lon,
                source: 'vesselfinder'
            }));
        });
    } catch (err) {
        console.error('Error VF:', err.message);
        broadcastAis(JSON.stringify({ type: 'diagnostico', msg: 'Error VesselFinder: ' + err.message }));
    }
}

function broadcastAis(text) {
    browserClients.forEach(c => { if (c.readyState === 1) c.send(text); });
}

wss.on('connection', (client) => {
    browserClients.add(client);
    client.on('close', () => browserClients.delete(client));
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor iniciado en puerto ${PORT}`);
    setInterval(inyectarBarcosVesselFinder, 300000);
    inyectarBarcosVesselFinder();
}).on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`Puerto ${PORT} ya en uso. En Azure no definas PORT manualmente en Configuracion; deja que la plataforma lo inyecte.`);
    }
    throw err;
});
