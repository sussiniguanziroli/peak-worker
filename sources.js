// Qué serie es "la potencia" de cada ubicación — mismo criterio que Panorama
// (iot-admin-panel: panoramaSchema.js, variableSelector.js, peakMonthlyModel.js).
//
// Candidatas, por ubicación:
//   1. homeConfig.panorama.sources.demand  (si panorama.completed y versión 1|2)
//   2. homeConfig.historicalTile.peakWidgetId (+ peakMachineId) — la designación
//      del Home anterior, que sigue en producción y lee el mismo documento.
// El documento peaks/monthly guarda UNA entrada por máquina: si las dos apuntan
// a la misma máquina gana Panorama.
//
// Solo se aceptan widgets en modo `simple` (o sin modo) con dataKey literal:
// es lo único que el ingestor guarda en BigQuery (extractValue), y es la clave
// que Panorama compara contra record.dataKey. Unidad W o kW, como exige Panorama.
const admin = require('firebase-admin');

const DEFAULT_TIMEZONE = 'America/Argentina/Buenos_Aires';
const SUPPORTED_PANORAMA_VERSIONS = [1, 2];
const DEMAND_UNITS = ['w', 'kw'];

const unitKey = (u) => String(u || '').toLowerCase().replace(/\s+/g, '');

const isValidTimeZone = (tz) => {
  if (!tz || typeof tz !== 'string') return false;
  try { new Intl.DateTimeFormat('en-US', { timeZone: tz }); return true; } catch { return false; }
};

// Resuelve una referencia {machineId, widgetId} contra layout.widgets. Devuelve
// la serie lista para consultar, o { skip: motivo }.
function resolveRef(ref, widgets) {
  if (!ref || !ref.widgetId) return { skip: 'sin widget' };
  const w = widgets.find(x => String(x.id) === String(ref.widgetId));
  if (!w) return { skip: `widget ${ref.widgetId} no existe` };
  const machineId = ref.machineId || w.machineId;
  if (!machineId || w.machineId !== machineId) return { skip: `widget ${ref.widgetId} no pertenece a ${machineId}` };
  const mode = w.payloadParsingMode || 'simple';
  if (mode !== 'simple') return { skip: `widget ${ref.widgetId} en modo ${mode} (no se guarda en BigQuery)` };
  if (!w.dataKey) return { skip: `widget ${ref.widgetId} sin dataKey` };
  if (!w.topic || w.topic.length <= 2) return { skip: `widget ${ref.widgetId} sin topic` };
  if (!DEMAND_UNITS.includes(unitKey(w.unit))) return { skip: `widget ${ref.widgetId} con unidad "${w.unit}" (se espera W o kW)` };
  return { machineId, widgetId: String(w.id), dataKey: w.dataKey, topic: w.topic, unit: w.unit };
}

/** @returns {Promise<Array<{tenantId, locationId, machineId, widgetId, dataKey, topic, unit, timeZone, origin}>>} */
async function loadSources() {
  const db = admin.firestore();
  const out = [];
  const tenants = await db.collection('tenants').listDocuments();
  for (const tenantRef of tenants) {
    const locations = await tenantRef.collection('locations').get();
    for (const locSnap of locations.docs) {
      const loc = locSnap.data() || {};
      const tag = `${tenantRef.id}/${locSnap.id}`;
      const hcSnap = await locSnap.ref.collection('config').doc('homeConfig').get();
      if (!hcSnap.exists) continue;
      const hc = hcSnap.data() || {};
      const widgets = loc.layout?.widgets || [];
      const timeZone = isValidTimeZone(loc.timezone) ? loc.timezone : DEFAULT_TIMEZONE;

      const candidates = [];
      const pano = hc.panorama;
      if (pano && pano.completed === true && SUPPORTED_PANORAMA_VERSIONS.includes(pano.version) && pano.sources?.demand) {
        candidates.push({ origin: 'panorama', ref: pano.sources.demand });
      }
      const ht = hc.historicalTile;
      if (ht?.peakWidgetId) {
        candidates.push({ origin: 'legacy', ref: { machineId: ht.peakMachineId, widgetId: ht.peakWidgetId } });
      }

      const seenMachines = new Set();
      for (const c of candidates) {
        const r = resolveRef(c.ref, widgets);
        if (r.skip) { console.log(`⏭️  [${tag}] ${c.origin}: ${r.skip}`); continue; }
        if (seenMachines.has(r.machineId)) continue;
        seenMachines.add(r.machineId);
        out.push({ tenantId: tenantRef.id, locationId: locSnap.id, timeZone, origin: c.origin, ...r });
      }
    }
  }
  return out;
}

module.exports = { loadSources };
