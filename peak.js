// Cálculo del pico sostenido de 15 min sobre realtime_telemetry.
//
// Definición (del usuario): el pico del mes es el MAYOR valor que la variable
// sostuvo durante 15 minutos seguidos. Para cada ventana [t, t+15min] se toma
// el MÍNIMO de las muestras; el pico es el máximo de esos mínimos. Si estuvo
// por encima de 500 durante 15 min y lo más bajo fue 495, el pico es 495 —
// no 600 aunque haya tocado 600 un instante.
//
// t recorre los instantes de cada muestra. La señal se toma como "sample and
// hold": cada muestra vale hasta la siguiente. Una ventana solo cuenta si no
// tiene huecos: ningún intervalo entre muestras consecutivas (incluido el que
// cruza el final de la ventana) supera MAX_GAP_S. Con huecos no se puede
// afirmar que el valor se sostuvo, así que esa ventana se descarta. La última
// ventana abierta (sin muestra posterior todavía) tampoco cuenta: se evalúa en
// la corrida siguiente.
const { BigQuery } = require('@google-cloud/bigquery');

const PROJECT_ID = 'iot-admin-panel';
const TABLES = {
  realtime: '`iot-admin-panel.iot_data.realtime_telemetry`',
  // Solo para el relleno de días que realtime ya no tiene (ver index.js,
  // REALTIME_COMPLETE_FROM): 1 muestra cada 5 min, así que el mínimo de la
  // ventana sale de 3–4 muestras y es una aproximación (nunca menor al real).
  raw:      '`iot-admin-panel.iot_data.raw_telemetry`',
};
const WINDOW_S   = 15 * 60;

const bigquery = new BigQuery({ projectId: PROJECT_ID });

const buildSql = (table) => `
  WITH s AS (
    SELECT timestamp AS ts,
           UNIX_MILLIS(timestamp) / 1000 AS sec,
           SAFE_CAST(value AS FLOAT64)   AS v
    FROM ${table}
    -- Por topic + data_key (como getRealtimeRange), no por machine_id: si dos
    -- widgets comparten topic y clave, el ingestor guarda solo el machine_id
    -- del primero.
    WHERE tenant_id   = @tenantId
      AND location_id = @locationId
      AND topic       = @topic
      AND data_key    = @dataKey
      AND timestamp  >= @from
      AND timestamp  <  @to
      AND SAFE_CAST(value AS FLOAT64) IS NOT NULL
  ),
  g AS (
    -- Sin muestra posterior → hueco "infinito": la ventana todavía no cerró.
    SELECT *, COALESCE(LEAD(sec) OVER (ORDER BY sec) - sec, 1e12) AS gap_next
    FROM s
  ),
  w AS (
    SELECT ts,
           MIN(v)        OVER fr AS wmin,
           MAX(gap_next) OVER fr AS max_gap
    FROM g
    WINDOW fr AS (ORDER BY sec RANGE BETWEEN CURRENT ROW AND ${WINDOW_S} FOLLOWING)
  )
  SELECT ts, wmin
  FROM w
  WHERE max_gap <= @maxGap
    AND ts < @windowTo
  ORDER BY wmin DESC, ts ASC
  LIMIT 1
`;

/**
 * Mejor ventana sostenida de una serie cuyo inicio cae en [from, windowTo).
 * Los datos se leen hasta `to` ("ahora"), así una ventana que arranca antes de
 * windowTo (fin de mes) puede cerrarse con muestras posteriores. Las ventanas
 * que todavía no cerraron se descartan solas.
 * @returns {Promise<{value:number, bucket:string, source:string}|null>}
 */
async function sustainedPeak({ tenantId, locationId, topic, dataKey, from, windowTo, to, maxGapS, source = 'realtime' }) {
  const [rows] = await bigquery.query({
    query: buildSql(TABLES[source]),
    location: 'us-central1',
    params: { tenantId, locationId, topic, dataKey, from, windowTo, to, maxGap: maxGapS },
    types: { from: 'TIMESTAMP', windowTo: 'TIMESTAMP', to: 'TIMESTAMP', maxGap: 'FLOAT64' },
    labels: { tenant: tenantId.toLowerCase().replace(/[^a-z0-9_-]/g, '_').slice(0, 63), app: 'peak-worker' },
  });
  if (!rows.length) return null;
  // ISO UTC con Z y milisegundos: el formato que Panorama acepta para `bucket`
  // (BigQuery devuelve microsegundos).
  return { value: rows[0].wmin, bucket: new Date(rows[0].ts.value).toISOString(), source };
}

module.exports = { sustainedPeak, WINDOW_S };
