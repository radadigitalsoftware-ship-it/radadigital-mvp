'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

const platformPort = process.env.PORT;
require('dotenv').config({ path: path.join(__dirname, '.env') });

const PORT = platformPort || process.env.PORT || 3000;
const MT_INTERVAL_MS = 60 * 1000;           // sincronizar cada 60 segundos
const COOKIE_RENOVAR_CADA = 3 * 60 * 60 * 1000; // renovar cookies cada 3 horas

// Tiles que cubren Dock Sud y alrededores al zoom 15
// Si necesitás ampliar el área, calculá más tiles en: https://tools.geofabrik.de/map/
const DOCK_SUD_TILES = [
    { z: 15, x: 5537, y: 9874 },
    { z: 15, x: 5538, y: 9874 },
    { z: 15, x: 5537, y: 9875 },
    { z: 15, x: 5538, y: 9875 },
];

// ─── Estado de cookies ────────────────────────────────────────────────────────
let cookiesActivas = process.env.MT_COOKIES || '';
let ultimaRenovacionCookies = 0;
let renovandoCookies = false;

// ─── Playwright: obtener cookies de MarineTraffic ─────────────────────────────
async function renovarCookiesMT() {
    if (renovandoCookies) return cookiesActivas;
    if (Date.now() - ultimaRenovacionCookies < COOKIE_RENOVAR_CADA && cookiesActivas) {
        return cookiesActivas;
    }

    renovandoCookies = true;
    console.log('[MT] Renovando cookies con Playwright...');

    let chromium;
    try {
        ({ chromium } = require('playwright-chromium'));
    } catch {
        console.warn('[MT] playwright-chromium no instalado. Usá las cookies del .env');
        renovandoCookies = false;
        return cookiesActivas;
    }

    let browser;
    try {
        browser = await chromium.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport: { width: 1280, height: 800 }
        });
        const page = await context.newPage();

        await page.goto(
            'https://www.marinetraffic.com/en/ais/home/centerx:-58.341/centery:-34.632/zoom:15',
            { waitUntil: 'networkidle', timeout: 45000 }
        );

        // Esperar que el mapa cargue y genere las cookies de sesión
        await page.waitForTimeout(5000);

        const cookies = await context.cookies();
        cookiesActivas = cookies
            .filter(c => c.domain.includes('marinetraffic.com'))
            .map(c => `${c.name}=${c.value}`)
            .join('; ');

        ultimaRenovacionCookies = Date.now();
        console.log(`[MT] Cookies renovadas OK (${cookies.length} cookies capturadas)`);

    } catch (err) {
        console.error('[MT] Error renovando cookies:', err.message);
        // Seguir usando las cookies viejas si fallan las nuevas
    } finally {
        if (browser) await browser.close().catch(() => {});
        renovandoCookies = false;
    }

    return cookiesActivas;
}

// ─── Fetch de un tile de MarineTraffic ───────────────────────────────────────
async function fetchTile(tile, cookies) {
    const url = `https://www.marinetraffic.com/getData/get_data_json_4/z:${tile.z}/X:${tile.x}/Y:${tile.y}/station:0`;
    const res = await fetch(url, {
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://www.marinetraffic.com/en/ais/home/centerx:-58.341/centery:-34.632/zoom:15',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'es-419,es;q=0.9',
            'Cookie': cookies,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        },
        signal: AbortSignal.timeout(15000) // timeout de 15s por tile
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}

// ─── Mapear formato MT → formato interno de la app ───────────────────────────
function mapearBarcoMT(b) {
    return {
        mmsi: b.SHIP_ID,          // tu app usa mmsi como ID único
        name: b.SHIPNAME,
        latitude: parseFloat(b.LAT),
        longitude: parseFloat(b.LON),
        sog: parseFloat(b.SPEED) || 0,
        course: parseFloat(b.COURSE) || 0,
        heading: b.HEADING ? parseInt(b.HEADING) : null,
        flag: b.FLAG,
        length: parseInt(b.LENGTH) || 0,
        width: parseInt(b.WIDTH) || 0,
        destination: b.DESTINATION || '',
        shipType: b.SHIPTYPE,
        source: 'marinetraffic'
    };
}

// ─── Sincronización principal ─────────────────────────────────────────────────
async function sincronizarBarcos() {
    console.log('[MT] Sincronizando...');
    try {
        const cookies = await renovarCookiesMT();

        if (!cookies) {
            console.warn('[MT] Sin cookies disponibles. Esperando renovación...');
            broadcastAis(JSON.stringify({
                type: 'diagnostico',
                msg: 'Obteniendo sesión de MarineTraffic, esperar unos segundos...'
            }));
            return;
        }

        // Todos los tiles EN PARALELO (4x más rápido que en serie)
        const respuestas = await Promise.all(
            DOCK_SUD_TILES.map(tile =>
                fetchTile(tile, cookies).catch(err => {
                    console.warn(`[MT] Error en tile ${tile.x},${tile.y}:`, err.message);
                    return null;
                })
            )
        );

        // Juntar todas las filas y deduplicar por SHIP_ID
        const vistos = new Set();
        const barcosBrutos = respuestas
            .filter(Boolean)
            .flatMap(data => data?.data?.rows || [])
            .filter(b => {
                if (!b.SHIP_ID || vistos.has(b.SHIP_ID)) return false;
                vistos.add(b.SHIP_ID);
                return true;
            });

        // Filtrar barcos con coordenadas válidas
        const barcos = barcosBrutos
            .filter(b => b.LAT && b.LON && !isNaN(parseFloat(b.LAT)))
            .map(mapearBarcoMT);

        broadcastAis(JSON.stringify({ type: 'barcos', barcos }));
        console.log(`[MT] OK: ${barcos.length} barcos en zona`);

        // Si hay muy pocos barcos, puede que las cookies estén vencidas
        if (barcos.length === 0 && barcosBrutos.length === 0) {
            console.warn('[MT] Sin barcos recibidos. Forzando renovación de cookies...');
            ultimaRenovacionCookies = 0; // forzar renovación en próximo ciclo
        }

    } catch (err) {
        console.error('[MT] Error general:', err.message);
        broadcastAis(JSON.stringify({
            type: 'diagnostico',
            msg: 'Error sincronizando MarineTraffic: ' + err.message
        }));
    }
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
const app = express();
app.use(express.static(path.join(__dirname, '..')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const browserClients = new Set();

function broadcastAis(text) {
    browserClients.forEach(c => {
        if (c.readyState === 1) c.send(text);
    });
}

wss.on('connection', (client) => {
    browserClients.add(client);
    client.send(JSON.stringify({ type: 'proxy_status', connected: true }));
    client.on('close', () => browserClients.delete(client));
});

// ─── Arranque ─────────────────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
    console.log(`[Rada] Servidor iniciado en puerto ${PORT}`);

    // Primera sincronización inmediata, luego cada MT_INTERVAL_MS
    sincronizarBarcos();
    setInterval(sincronizarBarcos, MT_INTERVAL_MS);

}).on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`Puerto ${PORT} ya en uso. En Azure no definas PORT manualmente.`);
    }
    throw err;
});
