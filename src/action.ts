// ingest-noaa-sst — daily NOAA ERDDAP SST ingest for the Chesapeake Bay.
// Pulls yesterday's water-temperature observations from NOAA CoastWatch's NDBC
// met dataset and commits Station + Observation things into the home repo.
import type { AddOperation, Operation } from "@warmhub/sdk-ts";
import { clientFromEnv, homeRepo, splitRepo } from "./warmhub";

const BBOX = { minLat: 33.5, maxLat: 39.7, minLon: -78.0, maxLon: -75.0 };
const ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/tabledap";
// NDBC standard meteorological, hourly — includes water temp (wtmp) when instrumented.
// See https://coastwatch.pfeg.noaa.gov/erddap/info/cwwcNDBCMet/
const DATASET = "cwwcNDBCMet";

interface ErddapResponse {
  table: {
    columnNames: string[];
    columnTypes: string[];
    columnUnits: string[];
    rows: unknown[][];
  };
}

interface ObservationRow {
  station: string;
  time: string;
  latitude: number;
  longitude: number;
  wtmp: number | null;  // water temperature, degrees C
}

function yesterdayIsoRange(): { start: string; end: string } {
  const now = new Date();
  const end = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

async function fetchObservations(): Promise<ObservationRow[]> {
  const { start, end } = yesterdayIsoRange();
  const vars = "station,time,latitude,longitude,wtmp";
  const query =
    `${vars}` +
    `&time>=${encodeURIComponent(start)}` +
    `&time<=${encodeURIComponent(end)}` +
    `&latitude>=${BBOX.minLat}&latitude<=${BBOX.maxLat}` +
    `&longitude>=${BBOX.minLon}&longitude<=${BBOX.maxLon}` +
    `&wtmp!=NaN`;
  const url = `${ERDDAP_BASE}/${DATASET}.json?${query}`;

  const token = process.env.NOAA_ERDDAP_TOKEN;
  const headers: Record<string, string> = { Accept: "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;

  const res = await fetch(url, { headers });
  // ERDDAP returns 404 when a filter has no rows — treat as empty, not an error.
  if (res.status === 404) return [];
  if (!res.ok) throw new Error(`ERDDAP ${res.status}: ${await res.text()}`);

  const body = (await res.json()) as ErddapResponse;
  const cols = body.table.columnNames;
  return body.table.rows.map((row) => {
    const rec: Record<string, unknown> = {};
    for (let i = 0; i < cols.length; i++) rec[cols[i]!] = row[i];
    return rec as unknown as ObservationRow;
  });
}

function buildOps(rows: ObservationRow[]): AddOperation[] {
  const seenStations = new Set<string>();
  const ops: AddOperation[] = [];

  for (const r of rows) {
    if (!seenStations.has(r.station)) {
      seenStations.add(r.station);
      ops.push({
        operation: "add",
        kind: "thing",
        name: `Station/${r.station}`,
        data: {
          station_id: r.station,
          lat: r.latitude,
          lon: r.longitude,
          operator: "NOAA-NDBC",
          kind: "buoy",
        },
        skipExisting: true,
      });
    }
  }

  for (const r of rows) {
    if (r.wtmp == null) continue;
    ops.push({
      operation: "add",
      kind: "thing",
      name: `Observation/${r.station}-${r.time}-sst`,
      data: {
        station: `Station/${r.station}`,
        timestamp: r.time,
        parameter: "sst_c",
        value: r.wtmp,
        unit: "C",
        quality_flag: "raw",
        source_system: "noaa-erddap-ndbc",
      },
      skipExisting: true,
    });
  }

  return ops;
}

async function main() {
  const client = clientFromEnv();
  const { orgName, repoName } = splitRepo(homeRepo());

  const rows = await fetchObservations();
  if (rows.length === 0) {
    console.log(JSON.stringify({ commitId: null, opCount: 0, note: "no rows returned" }));
    return;
  }

  const ops = buildOps(rows);
  const operations: Operation[] = ops;

  const today = new Date().toISOString().slice(0, 10);
  const result = await client.commit.apply(
    orgName,
    repoName,
    `NOAA SST daily ingest ${today}`,
    operations,
  );

  console.log(JSON.stringify({
    commitId: result.commitId,
    opCount: ops.length,
    stationsAdded: ops.filter((o) => o.name?.startsWith("Station/")).length,
    observationsAdded: ops.filter((o) => o.name?.startsWith("Observation/")).length,
  }));
}

main().catch((err) => { console.error(err); process.exit(1); });
