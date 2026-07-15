// Geo-Trade AI — WebSocket Manager™
// Service permanent (Railway) — maintient des connexions WebSocket ouvertes
// vers les exchanges et alimente en continu le Market Memory Engine™ partagé
// (même base Upstash Redis que le reste de Geo-Trade AI — une seule source
// de vérité, comme le prévoit l'architecture directrice).
//
// Binance et OKX : format vérifié et stable.
// Kraken et Coinbase Advanced : implémentation de bonne foi, format à valider
// en conditions réelles — le scan REST existant (market-radar.js) reste la
// source fiable pour ces deux exchanges tant que la validation n'est pas faite.

import WebSocket from 'ws';
import http from 'http';

const REST_URL = process.env.UPSTASH_REDIS_REST_URL;
const REST_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const SYMBOLES = ['BTC', 'ETH', 'SOL'];

async function kv(cmd) {
  const r = await fetch(REST_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REST_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd)
  });
  return r.json();
}

let dernierEtat = {}; // { 'binance:BTC': { prix, recuLe }, ... }
let statsConnexion = {}; // suivi de fiabilité par exchange

function noterEvenement(exchange, succes) {
  if (!statsConnexion[exchange]) statsConnexion[exchange] = { connexions: 0, deconnexions: 0, dernierEvenement: null };
  if (succes) statsConnexion[exchange].connexions++; else statsConnexion[exchange].deconnexions++;
  statsConnexion[exchange].dernierEvenement = new Date().toISOString();
}

async function enregistrerTick(exchange, symbole, prix) {
  const cle = `${exchange}:${symbole}`;
  dernierEtat[cle] = { prix, recuLe: new Date().toISOString() };
  // Valeur "live" toujours à jour, consultée par le frontend en temps réel.
  await kv(['SET', `geotrade:live:${symbole}:${exchange}`, JSON.stringify(dernierEtat[cle])]);
}

// Toutes les 5 minutes, un instantané de l'état courant alimente le Market
// Memory Engine™ existant (même structure que market-radar.js sur Vercel).
async function memoriserPeriodique() {
  for (const symbole of SYMBOLES) {
    const parExchange = {};
    for (const [cle, val] of Object.entries(dernierEtat)) {
      const [ex, sym] = cle.split(':');
      if (sym === symbole) parExchange[ex] = val.prix;
    }
    if (Object.keys(parExchange).length === 0) continue;
    const snapshot = { timestamp: new Date().toISOString(), source: 'websocket-manager', prix: parExchange };
    await kv(['ZADD', `geotrade:memory:${symbole}`, String(Date.now()), JSON.stringify(snapshot)]);
  }
  console.log('[Market Memory] Snapshot périodique enregistré —', new Date().toISOString());
}
setInterval(memoriserPeriodique, 5 * 60 * 1000);

// ── Reconnexion automatique générique ──
function connecterAvecReprise(nom, url, onOpen, onMessage) {
  function connecter() {
    console.log(`[${nom}] Connexion...`);
    const ws = new WebSocket(url);
    ws.on('open', () => { console.log(`[${nom}] ✓ Connecté`); noterEvenement(nom, true); onOpen(ws); });
    ws.on('message', (data) => { try { onMessage(JSON.parse(data.toString())); } catch (e) { console.error(`[${nom}] Erreur parsing:`, e.message); } });
    ws.on('close', () => { console.log(`[${nom}] Déconnecté — reprise dans 5s`); noterEvenement(nom, false); setTimeout(connecter, 5000); });
    ws.on('error', (e) => { console.error(`[${nom}] Erreur:`, e.message); });
  }
  connecter();
}

// ── Binance (format vérifié) ──
function demarrerBinance() {
  const streams = SYMBOLES.map(s => `${s.toLowerCase()}usdt@ticker`).join('/');
  connecterAvecReprise('binance', `wss://stream.binance.com:9443/stream?streams=${streams}`,
    () => {},
    (msg) => {
      const d = msg.data;
      if (!d || !d.s) return;
      const symbole = d.s.replace('USDT', '');
      enregistrerTick('binance', symbole, parseFloat(d.c));
    }
  );
}

// ── OKX (format vérifié) ──
function demarrerOKX() {
  connecterAvecReprise('okx', 'wss://ws.okx.com:8443/ws/v5/public',
    (ws) => {
      const args = SYMBOLES.map(s => ({ channel: 'tickers', instId: `${s}-USDT` }));
      ws.send(JSON.stringify({ op: 'subscribe', args }));
    },
    (msg) => {
      if (!msg.data?.[0]) return;
      const t = msg.data[0];
      const symbole = t.instId.split('-')[0];
      enregistrerTick('okx', symbole, parseFloat(t.last));
    }
  );
}

// ── Kraken (format à valider — implémentation de bonne foi, WS v2) ──
function demarrerKraken() {
  connecterAvecReprise('kraken', 'wss://ws.kraken.com/v2',
    (ws) => {
      const pairs = SYMBOLES.map(s => `${s}/USD`);
      ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ticker', symbol: pairs } }));
    },
    (msg) => {
      if (msg.channel !== 'ticker' || !msg.data?.[0]) return;
      const t = msg.data[0];
      const symbole = (t.symbol || '').split('/')[0];
      if (symbole && t.last) enregistrerTick('kraken', symbole, parseFloat(t.last));
    }
  );
}

// ── Coinbase Advanced (format à valider — implémentation de bonne foi) ──
function demarrerCoinbase() {
  connecterAvecReprise('coinbase_advanced', 'wss://advanced-trade-ws.coinbase.com',
    (ws) => {
      const product_ids = SYMBOLES.map(s => `${s}-USD`);
      ws.send(JSON.stringify({ type: 'subscribe', channel: 'ticker', product_ids }));
    },
    (msg) => {
      const events = msg.events?.[0]?.tickers;
      if (!events?.[0]) return;
      const t = events[0];
      const symbole = (t.product_id || '').split('-')[0];
      if (symbole && t.price) enregistrerTick('coinbase_advanced', symbole, parseFloat(t.price));
    }
  );
}

demarrerBinance();
demarrerOKX();
demarrerKraken();
demarrerCoinbase();

// ── Serveur HTTP minimal pour le health-check Railway ──
http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', etat: dernierEtat, fiabilite: statsConnexion }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Geo-Trade AI — WebSocket Manager actif');
}).listen(process.env.PORT || 3000, () => {
  console.log('Serveur health-check démarré sur le port', process.env.PORT || 3000);
});
