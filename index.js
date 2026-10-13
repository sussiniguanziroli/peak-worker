require('dotenv').config();
const admin = require('firebase-admin');
const { FieldPath } = require('firebase-admin/firestore');
const { sustainedPeak } = require('./peak');
const { loadSources } = require('./sources');

const PROJECT_ID = 'iot-admin-panel';

// DRY_RUN por defecto: calcula y loguea lo que escribiría, sin tocar Firestore.
// Pasar a escritura real con DRY_RUN=false en el .env.
const DRY_RUN     = process.env.DRY_RUN !== 'false';
const INTERVAL_MS = Number(process.env.INTERVAL_MIN || 60) * 60 * 1000;
// Cuánto hacia atrás mira cada corrida incremental. Tiene que superar
// INTERVAL + 15 min de ventana + un hueco, para que ninguna ventana quede sin
// evaluar entre dos corridas.
const LOOKBACK_MS = Number(process.env.LOOKBACK_MIN || 135) * 60 * 1000;
// Hueco máximo entre muestras dentro de una ventana para darla por sostenida.
const MAX_GAP_S   = Number(process.env.MAX_GAP_S || 120);
const ONCE        = process.argv.includes('--once');

// realtime_telemetry tenía vencimiento de 7 días hasta el 2026-10-13: la
// partición más vieja que sobrevivió es la del 2026-10-05 (UTC). Para lo
// anterior a esa fecha el pico se completa con raw_telemetry (1 muestra cada
// 5 min): mismo cálculo, con un hueco tolerado acorde a esa frecuencia. Desde
// noviembre de 2026 ningún mes recalculado empieza antes, y esto no corre.
const REALTIME_COMPLETE_FROM = new Date('2026-10-05T00:00:00Z');
const RAW_MAX_GAP_S = 420;

admin.initializeApp({ projectId: PROJECT_ID });
const db = admin.firestore();

// ── Fechas en la zona de la ubicación ────────────────────────────────────────
// Panorama compara record.month contra el mes vigente EN LA ZONA de la
// instalación ('YYYY-MM'), no en UTC.

function zonedParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone, hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  }).formatToParts(date);
  const get = (t) => Number(parts.find(p => p.type === t).value);
  return { y: get('year'), m: get('month'), d: get('day'), h: get('hour'), min: get('minute'), s: get('second') };
}

function zoneOffsetMs(date, timeZone) {
  const p = zonedParts(date, timeZone);
  return Date.UTC(p.y, p.m - 1, p.d, p.h, p.min, p.s) - Math.floor(date.getTime() / 1000) * 1000;
}

// Instante UTC del día 1, 00:00, del mes (y, m) en la zona dada.
function monthStartUtc(y, m, timeZone) {
  const guess = Date.UTC(y, m - 1, 1);
  let t = guess - zoneOffsetMs(new Date(guess), timeZone);
  const off2 = zoneOffsetMs(new Date(t), timeZone);
  if (guess - off2 !== t) t = guess - off2; // cambio de horario justo en el borde
  return new Date(t);
}

const monthKey = (y, m) => `${y}-${String(m).padStart(2, '0')}`;
const parseMonthKey = (k) => k.split('-').map(Number);
const nextMonth = (y, m) => (m === 12 ? [y + 1, 1] : [y, m + 1]);

// ── Estado en memoria ────────────────────────────────────────────────────────
// Sin persistencia propia (misma regla que los otros workers de la VM): al
// arrancar se recalcula el mes completo desde realtime_telemetry, que es la
// fuente de verdad. key tenant/location/machine →
//   { month, value, bucket, dataKey, topic, widgetId, unit, lastRunAt }
const state = new Map();

const better = (a, b) => {
  if (!a) return b;
  if (!b) return a;
  return b.value > a.value ? b : a;
};

const fmt = (v) => (typeof v === 'number' ? Math.round(v * 1000) / 1000 : v);

// ── Escritura ────────────────────────────────────────────────────────────────
// El worker es la autoridad del registro de su máquina: en cada corrida
// REAFIRMA current.{machineId} con su valor, aunque el documento tenga uno
// mayor. Mientras producción siga con el Home anterior, sus escritores
// (useMonthlyPeak / updatePeakIfHigher, "gana el mayor", desde el navegador)
// pueden pisarlo entre corridas; la corrida siguiente lo corrige.
async function assertCurrent(src, st) {
  const ref = db.doc(`tenants/${src.tenantId}/locations/${src.locationId}/peaks/monthly`);
  const record = {
    month: st.month,
    value: st.value,
    unit: src.unit,
    dataKey: src.dataKey,
    bucket: st.bucket,
    ts: Date.now(),
    method: st.source === 'raw' ? 'sustained-min-15m-raw5m' : 'sustained-min-15m',
    source: 'peak-worker',
  };
  const tag = `${src.tenantId}/${src.locationId}/${src.machineId}`;

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const cur = snap.exists ? snap.data()?.current?.[src.machineId] : undefined;
    const same = cur && cur.month === record.month && cur.value === record.value
      && cur.dataKey === record.dataKey && cur.unit === record.unit && cur.bucket === record.bucket;
    if (same) return;

    const prev = cur ? `${fmt(cur.value)} ${cur.unit || ''} (${cur.month}, ${cur.dataKey}, ${cur.source || 'legacy'})` : 'nada';
    console.log(`${DRY_RUN ? '🧪 [DRY_RUN] escribiría' : '✍️  escribe'} [${tag}] ${fmt(record.value)} ${record.unit} @ ${record.bucket} — antes: ${prev}`);
    if (DRY_RUN) return;

    if (snap.exists) tx.update(ref, new FieldPath('current', src.machineId), record);
    else tx.set(ref, { current: { [src.machineId]: record } }, { merge: true });
  });
}

// Al cerrar un mes se archiva el valor del worker en history.{YYYY-MM}.{machineId},
// con la misma forma que usaba el Home anterior.
async function archiveMonth(src, st) {
  const ref = db.doc(`tenants/${src.tenantId}/locations/${src.locationId}/peaks/monthly`);
  const entry = { value: st.value, ts: Date.now(), bucket: st.bucket, dataKey: src.dataKey };
  const tag = `${src.tenantId}/${src.locationId}/${src.machineId}`;
  console.log(`${DRY_RUN ? '🧪 [DRY_RUN] archivaría' : '🗄️  archiva'} [${tag}] ${st.month}: ${fmt(st.value)} ${src.unit}`);
  if (DRY_RUN) return;
  await ref.set({}, { merge: true });
  await ref.update(new FieldPath('history', st.month, src.machineId), entry);
}

// ── Corrida ──────────────────────────────────────────────────────────────────
async function processSource(src, now) {
  const key = `${src.tenantId}/${src.locationId}/${src.machineId}`;
  const { y, m } = zonedParts(now, src.timeZone);
  const month = monthKey(y, m);
  const monthStart = monthStartUtc(y, m, src.timeZone);
  const nextStart = monthStartUtc(...nextMonth(y, m), src.timeZone);
  const query = { tenantId: src.tenantId, locationId: src.locationId, topic: src.topic, dataKey: src.dataKey, to: now, maxGapS: MAX_GAP_S };

  let st = state.get(key);
  // Si cambió la variable designada, lo calculado no sirve: se recalcula.
  if (st && (st.dataKey !== src.dataKey || st.topic !== src.topic || st.widgetId !== src.widgetId)) {
    console.log(`🔁 [${key}] cambió la variable designada, se recalcula el mes`);
    st = null;
  }

  // Cambio de mes: última pasada sobre el mes anterior (ventanas que arrancaron
  // antes de la medianoche y cerraron después) y archivo.
  if (st && st.month !== month) {
    const [py, pm] = parseMonthKey(st.month);
    const prevStart = monthStartUtc(py, pm, src.timeZone);
    const from = new Date(Math.max(prevStart.getTime(), st.lastRunAt.getTime() - LOOKBACK_MS));
    const last = await sustainedPeak({ ...query, from, windowTo: monthStart });
    const closed = { ...st, ...(better(st.value != null ? st : null, last) || {}) };
    if (closed.value != null) await archiveMonth(src, closed);
    st = null;
  }

  const full = !st;
  let from = full ? monthStart : new Date(Math.max(monthStart.getTime(), now.getTime() - LOOKBACK_MS));
  let fromRaw = null;
  if (from < REALTIME_COMPLETE_FROM) {
    fromRaw = await sustainedPeak({
      ...query, source: 'raw', maxGapS: RAW_MAX_GAP_S,
      from, windowTo: new Date(Math.min(REALTIME_COMPLETE_FROM.getTime(), nextStart.getTime())),
    });
    from = REALTIME_COMPLETE_FROM;
  }
  const found = from < now ? await sustainedPeak({ ...query, from, windowTo: nextStart }) : null;
  const best = better(better(st && st.value != null ? st : null, fromRaw), found);

  st = {
    month, dataKey: src.dataKey, topic: src.topic, widgetId: src.widgetId, unit: src.unit, lastRunAt: now,
    value: best ? best.value : null, bucket: best ? best.bucket : null, source: best ? best.source : null,
  };
  state.set(key, st);

  if (full) console.log(`📊 [${key}] ${month} recalculado (${src.origin}, ${src.dataKey}): ${best ? `${fmt(best.value)} ${src.unit} @ ${best.bucket} [${best.source}]` : 'sin ventana sostenida'}${fromRaw ? ` (relleno raw antes del ${REALTIME_COMPLETE_FROM.toISOString().slice(0, 10)}: ${fmt(fromRaw.value)})` : ''}`);
  if (st.value != null) await assertCurrent(src, st);
}

let running = false;
async function runOnce() {
  if (running) { console.warn('⏳ Corrida anterior todavía en curso, se saltea'); return; }
  running = true;
  const now = new Date();
  try {
    const sources = await loadSources();
    console.log(`🔎 ${now.toISOString()} — ${sources.length} fuentes de potencia`);
    for (const src of sources) {
      try {
        await processSource(src, now);
      } catch (err) {
        console.error(`❌ [${src.tenantId}/${src.locationId}/${src.machineId}]`, err.message);
      }
    }
  } catch (err) {
    console.error('❌ Corrida fallida:', err.message);
  } finally {
    running = false;
  }
}

console.log(`🚀 peak-worker ${DRY_RUN ? '(DRY_RUN: no escribe)' : '(ESCRIBE en Firestore)'} — cada ${INTERVAL_MS / 60000} min, lookback ${LOOKBACK_MS / 60000} min, hueco máx ${MAX_GAP_S} s`);

if (ONCE) {
  runOnce().then(() => process.exit(0));
} else {
  runOnce();
  setInterval(runOnce, INTERVAL_MS);
}
