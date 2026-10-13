# peak-worker

Proceso standalone que calcula el **pico del mes** de la potencia de cada
ubicación desde BigQuery `realtime_telemetry` (todos los mensajes) y lo escribe
en `tenants/{t}/locations/{l}/peaks/monthly`, el documento que lee Panorama
(«Pico del mes» de la tarjeta Potencia) y el Home anterior.

## Definición del pico

El mayor valor que la potencia **sostuvo durante 15 minutos seguidos**: para
cada ventana de 15 min se toma el mínimo de las muestras, y el pico es el máximo
de esos mínimos. Si estuvo arriba de 500 kW durante 15 min y lo más bajo fue
495, el pico es 495 — no 600 aunque haya tocado 600 un instante.

Una ventana con un hueco de más de `MAX_GAP_S` entre muestras no cuenta: no se
puede afirmar que el valor se sostuvo. El cálculo está en `peak.js`.

## Qué serie se mide

Por ubicación (`sources.js`), con el mismo criterio que Panorama:

1. `config/homeConfig` → `panorama.sources.demand` (si Panorama está completo).
2. `config/homeConfig` → `historicalTile.peakWidgetId` (designación del Home anterior).

Solo widgets en modo `simple` con `dataKey`, unidad W o kW. Se escribe
`current.{machineId}` con `{ month, value, unit, dataKey, bucket, ts, method, source }`
(`month` en la zona de la ubicación, `bucket` = inicio de la ventana, ISO UTC).
Al cambiar de mes, el valor cerrado se archiva en `history.{YYYY-MM}.{machineId}`.

## Corridas

- Al arrancar: recalcula el mes en curso completo (sin estado en disco).
- Cada `INTERVAL_MIN`: mira las últimas `LOOKBACK_MIN` y actualiza si hay una
  ventana mejor.
- En cada corrida **reafirma** su valor en `current.{machineId}`: mientras
  producción tenga el Home anterior, sus escritores («gana el mayor», desde el
  navegador) pueden pisarlo; la corrida siguiente lo corrige.

Costo: ~10 MB facturados por serie y corrida; el recálculo de arranque lee el
mes de la serie (~30 MB por día transcurrido).

## Requisitos

- Node 18+
- Service account con Firestore y `roles/bigquery.jobUser` (se usa la del
  ingestor vía `GOOGLE_APPLICATION_CREDENTIALS`).

## Primer deploy en la VM

```bash
git clone https://github.com/sussiniguanziroli/peak-worker.git
cd peak-worker
npm install
cp .env.example .env        # DRY_RUN=true al principio
pm2 start ecosystem.config.js
pm2 save
pm2 logs peak-worker
```

Con `DRY_RUN=true` solo loguea `🧪 escribiría …`. Cuando los valores estén bien:
`DRY_RUN=false` en `.env` y `pm2 restart peak-worker`.

Prueba puntual sin dejarlo corriendo: `node index.js --once`.

## Actualizaciones

```bash
cd ~/peak-worker && git pull && npm install && pm2 restart peak-worker
```
