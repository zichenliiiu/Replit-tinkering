/**
 * sheets-charts.ts
 * Reads PLG and Sales-Led sheets, synthesizes insights, writes an "Insights"
 * summary tab with formatted tables, and embeds charts inside the spreadsheet.
 * Uses the Replit Google Sheets connector (google-sheet).
 *
 * Run with: pnpm --filter @workspace/scripts run sheets-charts
 */

import { ReplitConnectors } from "@replit/connectors-sdk";

const connectors = new ReplitConnectors();

// Target spreadsheet ID. Override via SPREADSHEET_ID env var.
// NOTE: The Sheets API v4 only works with native Google Sheets files, not
// Office/xlsx files stored in Drive. If the target document was originally
// uploaded as an Excel file, open it in Google Sheets and choose
// File → Save as Google Sheets to get a native copy, then pass that new ID
// via the SPREADSHEET_ID env variable.
const SPREADSHEET_ID =
  process.env["SPREADSHEET_ID"] ?? "1KxVeWddl-z1Pwhc_Ipdg4PEylQMk9AvK";

// ── Sheets API response types ─────────────────────────────────────────────

interface SheetsError {
  code: number;
  message: string;
  status: string;
}

interface GridProperties {
  rowCount: number;
  columnCount: number;
}

interface SheetProperties {
  sheetId: number;
  title: string;
  index: number;
  gridProperties: GridProperties;
}

interface EmbeddedChart {
  chartId: number;
}

interface Sheet {
  properties: SheetProperties;
  charts?: EmbeddedChart[];
}

interface SpreadsheetMetadata {
  properties?: { title: string };
  sheets?: Sheet[];
  error?: SheetsError;
}

interface ValuesResponse {
  values?: string[][];
  error?: SheetsError;
}

interface BatchUpdateResponse {
  replies?: Array<{ addSheet?: { properties: SheetProperties } }>;
  error?: SheetsError;
}

// ── Sheets API request shape types ────────────────────────────────────────

interface GridRange {
  sheetId: number;
  startRowIndex?: number;
  endRowIndex?: number;
  startColumnIndex?: number;
  endColumnIndex?: number;
}

interface CellFormat {
  textFormat?: {
    bold?: boolean;
    fontSize?: number;
    foregroundColor?: { red: number; green: number; blue: number };
  };
  backgroundColor?: { red: number; green: number; blue: number };
  numberFormat?: { type: string; pattern: string };
}

interface RepeatCellRequest {
  repeatCell: {
    range: GridRange;
    cell: { userEnteredFormat: CellFormat };
    fields: string;
  };
}

interface UpdateCellsRequest {
  updateCells: {
    range: GridRange;
    fields: string;
  };
}

interface UpdateDimensionRequest {
  updateDimensionProperties: {
    range: { sheetId: number; dimension: string; startIndex: number; endIndex: number };
    properties: { pixelSize: number };
    fields: string;
  };
}

interface DeleteObjectRequest {
  deleteEmbeddedObject: { objectId: number };
}

interface AddSheetRequest {
  addSheet: {
    properties: { title: string; index: number; gridProperties: GridProperties };
  };
}

interface SourceRange {
  sources: Array<{
    sheetId: number;
    startRowIndex: number;
    endRowIndex: number;
    startColumnIndex: number;
    endColumnIndex: number;
  }>;
}

interface ChartDataSource {
  sourceRange: SourceRange;
}

interface BasicChartAxis {
  position: "BOTTOM_AXIS" | "LEFT_AXIS" | "RIGHT_AXIS";
  title?: string;
}

interface BasicChartDomain {
  domain: ChartDataSource;
}

interface BasicChartSeries {
  series: ChartDataSource;
  targetAxis?: "BOTTOM_AXIS" | "LEFT_AXIS";
  color?: { red: number; green: number; blue: number };
}

interface BasicChartSpec {
  chartType: "COLUMN" | "BAR" | "LINE" | "AREA";
  legendPosition: "NO_LEGEND" | "BOTTOM_LEGEND" | "TOP_LEGEND" | "LEFT_LEGEND" | "RIGHT_LEGEND";
  axis: BasicChartAxis[];
  domains: BasicChartDomain[];
  series: BasicChartSeries[];
  headerCount: number;
}

interface PieChartSpec {
  legendPosition: "RIGHT_LEGEND" | "BOTTOM_LEGEND" | "LEFT_LEGEND";
  threeDimensional: boolean;
  domain: ChartDataSource;
  series: ChartDataSource;
}

interface ChartSpec {
  title: string;
  titleTextFormat: { bold: boolean; fontSize: number };
  basicChart?: BasicChartSpec;
  pieChart?: PieChartSpec;
}

interface AnchorCell {
  sheetId: number;
  rowIndex: number;
  columnIndex: number;
}

interface OverlayPosition {
  anchorCell: AnchorCell;
  widthPixels: number;
  heightPixels: number;
}

interface ChartPosition {
  overlayPosition: OverlayPosition;
}

interface EmbeddedChartSpec {
  spec: ChartSpec;
  position: ChartPosition;
}

interface AddChartRequest {
  addChart: { chart: EmbeddedChartSpec };
}

type BatchRequest =
  | RepeatCellRequest
  | UpdateCellsRequest
  | UpdateDimensionRequest
  | DeleteObjectRequest
  | AddSheetRequest
  | AddChartRequest;

// ── Helpers ───────────────────────────────────────────────────────────────

function parseNum(val: string | undefined): number {
  if (!val) return 0;
  return parseFloat(val.replace(/[$,%]/g, "").replace(/,/g, "")) || 0;
}

function fmt(n: number): string {
  return "$" + Math.round(n).toLocaleString("en-US");
}

function assertNativeSheet(err: SheetsError): void {
  if (
    err.status === "FAILED_PRECONDITION" &&
    err.message?.includes("Office file")
  ) {
    console.error(
      "\n❌  The target spreadsheet is stored as an Office/xlsx file.\n" +
      "    The Sheets API can only add charts to native Google Sheets.\n\n" +
      "    To fix this:\n" +
      "      1. Open the spreadsheet in Google Sheets.\n" +
      "      2. Choose  File → Save as Google Sheets.\n" +
      "      3. Copy the new spreadsheet ID from the URL.\n" +
      "      4. Re-run:  SPREADSHEET_ID=<new-id> pnpm --filter @workspace/scripts run sheets-charts\n"
    );
    process.exit(1);
  }
}

async function getValues(sheetName: string): Promise<string[][]> {
  const resp = await connectors.proxy(
    "google-sheet",
    `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(sheetName)}`,
    { method: "GET" }
  );
  const d = (await resp.json()) as ValuesResponse;
  if (d.error) {
    assertNativeSheet(d.error);
    throw new Error(`getValues(${sheetName}): ${JSON.stringify(d.error)}`);
  }
  return d.values ?? [];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function safeJson<T>(resp: Response, label: string): Promise<T> {
  const text = await resp.text();
  try {
    return JSON.parse(text) as T;
  } catch {
    // Likely a rate-limit HTML page — surface a useful error
    throw new Error(`${label}: non-JSON response (status ${resp.status}) — ${text.slice(0, 200)}`);
  }
}

async function batchUpdateSheet(requests: BatchRequest[]): Promise<BatchUpdateResponse> {
  const MAX_RETRIES = 5;
  let delay = 3000;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const resp = await connectors.proxy(
      "google-sheet",
      `/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requests }),
      }
    );
    if (resp.status === 429 || resp.status === 503) {
      if (attempt === MAX_RETRIES) throw new Error(`batchUpdate: rate limited after ${MAX_RETRIES} retries`);
      console.warn(`  ⏳ Rate limited (${resp.status}) — retrying in ${delay / 1000}s…`);
      await sleep(delay);
      delay *= 2;
      continue;
    }
    const d = await safeJson<BatchUpdateResponse>(resp, "batchUpdate");
    if (d.error) throw new Error(`batchUpdate failed: ${JSON.stringify(d.error)}`);
    return d;
  }
  throw new Error("batchUpdate: exhausted retries");
}

async function writeValues(range: string, values: (string | number)[][]): Promise<void> {
  const MAX_RETRIES = 5;
  let delay = 3000;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const resp = await connectors.proxy(
      "google-sheet",
      `/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ range, values }),
      }
    );
    if (resp.status === 429 || resp.status === 503) {
      if (attempt === MAX_RETRIES) throw new Error(`writeValues: rate limited after ${MAX_RETRIES} retries`);
      console.warn(`  ⏳ Rate limited (${resp.status}) — retrying in ${delay / 1000}s…`);
      await sleep(delay);
      delay *= 2;
      continue;
    }
    const d = await safeJson<ValuesResponse>(resp, `writeValues(${range})`);
    if (d.error) throw new Error(`writeValues(${range}): ${JSON.stringify(d.error)}`);
    return;
  }
}

// ── Aggregate helpers ─────────────────────────────────────────────────────

function aggregateBy(
  rows: string[][],
  keyCol: number,
  valueCol: number,
  countMap?: Map<string, number>
): Map<string, number> {
  const map = new Map<string, number>();
  for (const row of rows) {
    const key = row[keyCol] ?? "Unknown";
    const val = parseNum(row[valueCol]);
    map.set(key, (map.get(key) ?? 0) + val);
    if (countMap) countMap.set(key, (countMap.get(key) ?? 0) + 1);
  }
  return map;
}

function sortedDesc(map: Map<string, number>): [string, number][] {
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

// ── Sheet management ──────────────────────────────────────────────────────

async function getOrCreateSheet(name: string, tabIndex = 0): Promise<number> {
  const resp = await connectors.proxy(
    "google-sheet",
    `/v4/spreadsheets/${SPREADSHEET_ID}?includeGridData=false`,
    { method: "GET" }
  );
  const meta = (await resp.json()) as SpreadsheetMetadata;
  if (meta.error) {
    assertNativeSheet(meta.error);
    throw new Error(`getMetadata: ${JSON.stringify(meta.error)}`);
  }

  const existing = (meta.sheets ?? []).find((s) => s.properties.title === name);

  if (existing) {
    const sheetId = existing.properties.sheetId;
    console.log(`Clearing existing ${name} sheet...`);
    const clearRequests: BatchRequest[] = [
      ...(existing.charts ?? []).map(
        (c): DeleteObjectRequest => ({
          deleteEmbeddedObject: { objectId: c.chartId },
        })
      ),
      {
        updateCells: {
          range: { sheetId, startRowIndex: 0, startColumnIndex: 0 },
          fields: "userEnteredValue,userEnteredFormat",
        },
      },
    ];
    await batchUpdateSheet(clearRequests);
    return sheetId;
  }

  const addResp = await batchUpdateSheet([
    {
      addSheet: {
        properties: {
          title: name,
          index: tabIndex,
          gridProperties: { rowCount: 300, columnCount: 20 },
        },
      },
    },
  ]);
  const newSheetId = addResp.replies?.[0]?.addSheet?.properties?.sheetId;
  if (newSheetId === undefined) throw new Error(`Failed to get new sheet ID for ${name}`);
  console.log(`Created ${name} sheet, id:`, newSheetId);
  return newSheetId;
}

// ── Formatting builders ───────────────────────────────────────────────────

function boldRow(sheetId: number, row: number, startCol: number, endCol: number): RepeatCellRequest {
  return {
    repeatCell: {
      range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: startCol, endColumnIndex: endCol },
      cell: { userEnteredFormat: { textFormat: { bold: true } } },
      fields: "userEnteredFormat.textFormat.bold",
    },
  };
}

function bgRow(
  sheetId: number,
  row: number,
  startCol: number,
  endCol: number,
  r: number,
  g: number,
  b: number
): RepeatCellRequest {
  return {
    repeatCell: {
      range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: startCol, endColumnIndex: endCol },
      cell: { userEnteredFormat: { backgroundColor: { red: r, green: g, blue: b } } },
      fields: "userEnteredFormat.backgroundColor",
    },
  };
}

function sectionHeader(sheetId: number, row: number, cols: number): BatchRequest[] {
  return [
    bgRow(sheetId, row, 0, cols, 0.22, 0.44, 0.78),
    boldRow(sheetId, row, 0, cols),
    {
      repeatCell: {
        range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: 0, endColumnIndex: cols },
        cell: {
          userEnteredFormat: {
            textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } },
          },
        },
        fields: "userEnteredFormat.textFormat",
      },
    } satisfies RepeatCellRequest,
  ];
}

function tableHeader(sheetId: number, row: number, cols: number): BatchRequest[] {
  return [bgRow(sheetId, row, 0, cols, 0.85, 0.91, 0.97), boldRow(sheetId, row, 0, cols)];
}

function numFmtRange(
  sheetId: number,
  startRow: number,
  endRow: number,
  startCol: number,
  endCol: number,
  pattern: string
): RepeatCellRequest {
  return {
    repeatCell: {
      range: { sheetId, startRowIndex: startRow, endRowIndex: endRow, startColumnIndex: startCol, endColumnIndex: endCol },
      cell: { userEnteredFormat: { numberFormat: { type: "NUMBER", pattern } } },
      fields: "userEnteredFormat.numberFormat",
    },
  };
}

function columnWidth(sheetId: number, colIdx: number, pixels: number): UpdateDimensionRequest {
  return {
    updateDimensionProperties: {
      range: { sheetId, dimension: "COLUMNS", startIndex: colIdx, endIndex: colIdx + 1 },
      properties: { pixelSize: pixels },
      fields: "pixelSize",
    },
  };
}

// ── Chart builders ────────────────────────────────────────────────────────

interface ChartPlacement {
  sheetId: number;     // sheet where data lives
  headerRow: number;
  dataRows: number;
  catCol: number;
  valCol: number;
  anchorRow: number;
  anchorCol: number;
  chartSheetId: number;
  widthPx?: number;
  heightPx?: number;
}

function lineChart(title: string, p: ChartPlacement): AddChartRequest {
  const { sheetId, headerRow, dataRows, catCol, valCol, anchorRow, anchorCol, chartSheetId } = p;
  const widthPx = p.widthPx ?? 520;
  const heightPx = p.heightPx ?? 300;
  const endRow = headerRow + 1 + dataRows;
  return {
    addChart: {
      chart: {
        spec: {
          title,
          titleTextFormat: { bold: true, fontSize: 13 },
          basicChart: {
            chartType: "LINE",
            legendPosition: "NO_LEGEND",
            axis: [
              { position: "BOTTOM_AXIS" },
              { position: "LEFT_AXIS", title: "ARR ($)" },
            ],
            domains: [
              {
                domain: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: catCol, endColumnIndex: catCol + 1 }],
                  },
                },
              },
            ],
            series: [
              {
                series: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: valCol, endColumnIndex: valCol + 1 }],
                  },
                },
                targetAxis: "LEFT_AXIS",
                color: { red: 0.22, green: 0.44, blue: 0.78 },
              },
            ],
            headerCount: 1,
          },
        },
        position: {
          overlayPosition: {
            anchorCell: { sheetId: chartSheetId, rowIndex: anchorRow, columnIndex: anchorCol },
            widthPixels: widthPx,
            heightPixels: heightPx,
          },
        },
      },
    },
  };
}

function multiSeriesColChart(
  title: string,
  sheetId: number,
  headerRow: number,
  dataRows: number,
  catCol: number,
  seriesCols: number[],
  seriesColors: Array<{ red: number; green: number; blue: number }>,
  anchorRow: number,
  anchorCol: number,
  chartSheetId: number,
  widthPx = 520,
  heightPx = 300
): AddChartRequest {
  const endRow = headerRow + 1 + dataRows;
  return {
    addChart: {
      chart: {
        spec: {
          title,
          titleTextFormat: { bold: true, fontSize: 13 },
          basicChart: {
            chartType: "COLUMN",
            legendPosition: "BOTTOM_LEGEND",
            axis: [
              { position: "BOTTOM_AXIS" },
              { position: "LEFT_AXIS", title: "Seats" },
            ],
            domains: [
              {
                domain: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: catCol, endColumnIndex: catCol + 1 }],
                  },
                },
              },
            ],
            series: seriesCols.map((col, i) => ({
              series: {
                sourceRange: {
                  sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: col, endColumnIndex: col + 1 }],
                },
              },
              targetAxis: "LEFT_AXIS" as const,
              color: seriesColors[i] ?? { red: 0.22, green: 0.44, blue: 0.78 },
            })),
            headerCount: 1,
          },
        },
        position: {
          overlayPosition: {
            anchorCell: { sheetId: chartSheetId, rowIndex: anchorRow, columnIndex: anchorCol },
            widthPixels: widthPx,
            heightPixels: heightPx,
          },
        },
      },
    },
  };
}

function colChart(title: string, p: ChartPlacement): AddChartRequest {
  const { sheetId, headerRow, dataRows, catCol, valCol, anchorRow, anchorCol, chartSheetId } = p;
  const widthPx = p.widthPx ?? 520;
  const heightPx = p.heightPx ?? 300;
  const endRow = headerRow + 1 + dataRows;
  return {
    addChart: {
      chart: {
        spec: {
          title,
          titleTextFormat: { bold: true, fontSize: 13 },
          basicChart: {
            chartType: "COLUMN",
            legendPosition: "NO_LEGEND",
            axis: [
              { position: "BOTTOM_AXIS" },
              { position: "LEFT_AXIS", title: "ARR ($)" },
            ],
            domains: [
              {
                domain: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: catCol, endColumnIndex: catCol + 1 }],
                  },
                },
              },
            ],
            series: [
              {
                series: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: valCol, endColumnIndex: valCol + 1 }],
                  },
                },
                targetAxis: "LEFT_AXIS",
                color: { red: 0.22, green: 0.44, blue: 0.78 },
              },
            ],
            headerCount: 1,
          },
        },
        position: {
          overlayPosition: {
            anchorCell: { sheetId: chartSheetId, rowIndex: anchorRow, columnIndex: anchorCol },
            widthPixels: widthPx,
            heightPixels: heightPx,
          },
        },
      },
    },
  };
}

function barChart(title: string, p: ChartPlacement): AddChartRequest {
  const { sheetId, headerRow, dataRows, catCol, valCol, anchorRow, anchorCol, chartSheetId } = p;
  const widthPx = p.widthPx ?? 520;
  const heightPx = p.heightPx ?? 380;
  const endRow = headerRow + 1 + dataRows;
  return {
    addChart: {
      chart: {
        spec: {
          title,
          titleTextFormat: { bold: true, fontSize: 13 },
          basicChart: {
            chartType: "BAR",
            legendPosition: "NO_LEGEND",
            axis: [
              { position: "BOTTOM_AXIS", title: "ARR ($)" },
              { position: "LEFT_AXIS" },
            ],
            domains: [
              {
                domain: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: catCol, endColumnIndex: catCol + 1 }],
                  },
                },
              },
            ],
            series: [
              {
                series: {
                  sourceRange: {
                    sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: valCol, endColumnIndex: valCol + 1 }],
                  },
                },
                targetAxis: "BOTTOM_AXIS",
                color: { red: 0.22, green: 0.44, blue: 0.78 },
              },
            ],
            headerCount: 1,
          },
        },
        position: {
          overlayPosition: {
            anchorCell: { sheetId: chartSheetId, rowIndex: anchorRow, columnIndex: anchorCol },
            widthPixels: widthPx,
            heightPixels: heightPx,
          },
        },
      },
    },
  };
}

function pieChart(title: string, p: ChartPlacement): AddChartRequest {
  const { sheetId, headerRow, dataRows, catCol, valCol, anchorRow, anchorCol, chartSheetId } = p;
  const widthPx = p.widthPx ?? 440;
  const heightPx = p.heightPx ?? 300;
  const endRow = headerRow + 1 + dataRows;
  return {
    addChart: {
      chart: {
        spec: {
          title,
          titleTextFormat: { bold: true, fontSize: 13 },
          pieChart: {
            legendPosition: "RIGHT_LEGEND",
            threeDimensional: false,
            domain: {
              sourceRange: {
                sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: catCol, endColumnIndex: catCol + 1 }],
              },
            },
            series: {
              sourceRange: {
                sources: [{ sheetId, startRowIndex: headerRow, endRowIndex: endRow, startColumnIndex: valCol, endColumnIndex: valCol + 1 }],
              },
            },
          },
        },
        position: {
          overlayPosition: {
            anchorCell: { sheetId: chartSheetId, rowIndex: anchorRow, columnIndex: anchorCol },
            widthPixels: widthPx,
            heightPixels: heightPx,
          },
        },
      },
    },
  };
}

// ── Forecast Tab ──────────────────────────────────────────────────────────
//
// Structure:
//  • Assumption blocks (highlighted yellow) — prices, switch rates, expansion rates
//  • Current user base — live SUMIF from source sheets
//  • Expansion pool — SL from data, PLG estimated via coverage ratio
//  • Scenario results — Conservative / Base / Aggressive for switch + expansion ARR
//  • Sensitivity table — Net New ARR across price × SL adoption rate grid

async function buildForecastTab(_plgRows: string[][], _slRows: string[][]): Promise<void> {
  console.log("\nBuilding Forecast tab...");
  const forecastSheetId = await getOrCreateSheet("Forecast", 2);

  type FRow = (string | number)[];
  const grid: FRow[] = [];
  const yellowCells: Array<[number, number, number, number]> = []; // 0-based [sr,er,sc,ec]
  const pctCells:    Array<[number, number, number, number]> = [];

  function addRow(cols: FRow = []): void { grid.push(cols); }
  function markYellow(sc: number, ec: number): void {
    yellowCells.push([grid.length - 1, grid.length, sc, ec]);
  }
  function markPct(sc: number, ec: number): void {
    pctCells.push([grid.length - 1, grid.length, sc, ec]);
  }

  // ── Title ─────────────────────────────────────────────────────────────
  addRow(["MARKETING LICENSE — REVENUE FORECAST MODEL"]);
  addRow([]);

  // ── PRICE ASSUMPTIONS ─────────────────────────────────────────────────
  addRow(["LICENSE PRICE ASSUMPTIONS  —  edit highlighted cells"]);
  addRow(["Parameter", "Value ($/user/month)", "Notes"]);
  const priceHdrIdx = grid.length; // 0-indexed; sheet row = idx+1
  addRow(["Marketing License (new)", 20, "Your key pricing lever — the new SKU"]);
  markYellow(1, 2);
  addRow(["Full Seat (current)", 45, "Update to your actual pricing"]);
  markYellow(1, 2);
  addRow(["Dev Seat (current)", 35, "Update to your actual pricing"]);
  markYellow(1, 2);
  addRow(["Collab Seat (current)", 15, "Update to your actual pricing"]);
  markYellow(1, 2);
  addRow(["Viewer Seat (current)", 5, "Update to your actual pricing"]);
  markYellow(1, 2);
  addRow([]);

  // 1-based sheet row refs for price cells (col B)
  const mktgPriceSR   = priceHdrIdx + 1; // sheet row of Marketing License price
  const fullPriceSR   = priceHdrIdx + 2;
  const devPriceSR    = priceHdrIdx + 3;
  const collabPriceSR = priceHdrIdx + 4;
  const viewerPriceSR = priceHdrIdx + 5;

  // ── SWITCH RATE ASSUMPTIONS ───────────────────────────────────────────
  addRow(["SWITCH RATE ASSUMPTIONS  —  % of current mktg users on each seat who switch to the new license"]);
  addRow(["Seat Type", "Conservative", "Base", "Aggressive", "Rationale"]);
  const switchHdrIdx = grid.length;
  addRow(["Full → Marketing",   0.10, 0.20, 0.35, "Mktg license cheaper → strong financial incentive to switch"]);
  markYellow(1, 4); markPct(1, 4);
  addRow(["Dev → Marketing",    0.08, 0.15, 0.25, "Cheaper alternative for dev-seat marketers"]);
  markYellow(1, 4); markPct(1, 4);
  addRow(["Collab → Marketing", 0.05, 0.10, 0.20, "Mktg license pricier → only upgrade for mktg-specific features"]);
  markYellow(1, 4); markPct(1, 4);
  addRow(["Viewer → Marketing", 0.02, 0.05, 0.10, "Mktg license much pricier → only highly engaged viewers upgrade"]);
  markYellow(1, 4); markPct(1, 4);
  addRow([]);

  const fullSwitchSR   = switchHdrIdx + 1; // sheet rows for switch rate rows
  const devSwitchSR    = switchHdrIdx + 2;
  const collabSwitchSR = switchHdrIdx + 3;
  const viewerSwitchSR = switchHdrIdx + 4;

  // ── EXPANSION ASSUMPTIONS ─────────────────────────────────────────────
  addRow(["EXPANSION ASSUMPTIONS  —  unadopted marketing employees who sign up for the new license"]);
  addRow(["Parameter", "Conservative", "Base", "Aggressive", "Notes"]);
  const expHdrIdx = grid.length;
  addRow(["SL: Expansion Adoption Rate", 0.05, 0.15, 0.25,
    "% of unadopted SL mktg employees (Marketing Count − current users) who sign up"]);
  markYellow(1, 4); markPct(1, 4);
  addRow(["PLG: Mktg Coverage Ratio", 0.30, 0.20, 0.15,
    "Assumed % of total mktg employees at PLG companies already in product"]);
  markYellow(1, 4); markPct(1, 4);
  addRow(["PLG: Expansion Adoption Rate", 0.05, 0.12, 0.20,
    "% of unadopted PLG mktg employees who sign up"]);
  markYellow(1, 4); markPct(1, 4);
  addRow([]);

  const slExpSR       = expHdrIdx + 1;
  const plgCoverageSR = expHdrIdx + 2;
  const plgExpSR      = expHdrIdx + 3;

  // ── CURRENT MARKETING USER BASE ───────────────────────────────────────
  addRow(["CURRENT MARKETING USER BASE  —  live from source data"]);
  addRow(["Motion", "Full", "Dev", "Collab", "Viewer", "Total Mktg Users"]);
  const dataHdrIdx = grid.length;
  const plgDataSR  = dataHdrIdx + 1;
  const slDataSR   = dataHdrIdx + 2;

  addRow(["PLG",
    "=SUM(PLG!I:I)", "=SUM(PLG!J:J)", "=SUM(PLG!K:K)", "=SUM(PLG!L:L)",
    `=B${plgDataSR}+C${plgDataSR}+D${plgDataSR}+E${plgDataSR}`,
  ]);
  addRow(["Sales-Led",
    "=SUM('Sales-Led'!R:R)", "=SUM('Sales-Led'!S:S)",
    "=SUM('Sales-Led'!T:T)", "=SUM('Sales-Led'!U:U)",
    `=B${slDataSR}+C${slDataSR}+D${slDataSR}+E${slDataSR}`,
  ]);
  addRow(["Combined",
    `=B${plgDataSR}+B${slDataSR}`, `=C${plgDataSR}+C${slDataSR}`,
    `=D${plgDataSR}+D${slDataSR}`, `=E${plgDataSR}+E${slDataSR}`,
    `=F${plgDataSR}+F${slDataSR}`,
  ]);
  addRow([]);

  // ── EXPANSION POOL ────────────────────────────────────────────────────
  addRow(["EXPANSION POOL  —  est. marketing employees not yet using the product"]);
  addRow(["Motion", "Est. Total Mktg Employees", "Current Mktg Users", "Unadopted Pool", "Notes"]);
  const poolHdrIdx = grid.length;
  const slPoolSR   = poolHdrIdx + 1;
  const plgPoolSR  = poolHdrIdx + 2;

  addRow(["Sales-Led",
    "=SUM('Sales-Led'!L:L)",
    `=F${slDataSR}`,
    `=B${slPoolSR}-C${slPoolSR}`,
    "From data: Marketing Count (employees at company)",
  ]);
  addRow(["PLG (Base estimate)",
    `=ROUND(F${plgDataSR}/C${plgCoverageSR},0)`,
    `=F${plgDataSR}`,
    `=B${plgPoolSR}-C${plgPoolSR}`,
    "Estimated: current mktg users ÷ Base coverage ratio",
  ]);
  addRow(["Combined",
    `=B${slPoolSR}+B${plgPoolSR}`,
    `=C${slPoolSR}+C${plgPoolSR}`,
    `=D${slPoolSR}+D${plgPoolSR}`, "",
  ]);
  addRow([]);

  // ── SCENARIO RESULTS ──────────────────────────────────────────────────
  addRow(["SCENARIO RESULTS  —  annual revenue impact of launching the Marketing license"]);
  addRow(["Revenue Component", "Conservative", "Base", "Aggressive"]);

  addRow(["SWITCH EFFECT  (revenue delta from existing mktg users switching seats)"]);
  const fullSwIdx   = grid.length;
  addRow(["Full → Marketing",
    `=(B${plgDataSR}+B${slDataSR})*B${fullSwitchSR}*(B${mktgPriceSR}-B${fullPriceSR})*12`,
    `=(B${plgDataSR}+B${slDataSR})*C${fullSwitchSR}*(B${mktgPriceSR}-B${fullPriceSR})*12`,
    `=(B${plgDataSR}+B${slDataSR})*D${fullSwitchSR}*(B${mktgPriceSR}-B${fullPriceSR})*12`,
  ]);
  const devSwIdx    = grid.length;
  addRow(["Dev → Marketing",
    `=(C${plgDataSR}+C${slDataSR})*B${devSwitchSR}*(B${mktgPriceSR}-B${devPriceSR})*12`,
    `=(C${plgDataSR}+C${slDataSR})*C${devSwitchSR}*(B${mktgPriceSR}-B${devPriceSR})*12`,
    `=(C${plgDataSR}+C${slDataSR})*D${devSwitchSR}*(B${mktgPriceSR}-B${devPriceSR})*12`,
  ]);
  const collabSwIdx = grid.length;
  addRow(["Collab → Marketing",
    `=(D${plgDataSR}+D${slDataSR})*B${collabSwitchSR}*(B${mktgPriceSR}-B${collabPriceSR})*12`,
    `=(D${plgDataSR}+D${slDataSR})*C${collabSwitchSR}*(B${mktgPriceSR}-B${collabPriceSR})*12`,
    `=(D${plgDataSR}+D${slDataSR})*D${collabSwitchSR}*(B${mktgPriceSR}-B${collabPriceSR})*12`,
  ]);
  const viewerSwIdx = grid.length;
  addRow(["Viewer → Marketing",
    `=(E${plgDataSR}+E${slDataSR})*B${viewerSwitchSR}*(B${mktgPriceSR}-B${viewerPriceSR})*12`,
    `=(E${plgDataSR}+E${slDataSR})*C${viewerSwitchSR}*(B${mktgPriceSR}-B${viewerPriceSR})*12`,
    `=(E${plgDataSR}+E${slDataSR})*D${viewerSwitchSR}*(B${mktgPriceSR}-B${viewerPriceSR})*12`,
  ]);
  addRow(["Total Switch Effect",
    `=SUM(B${fullSwIdx+1}:B${viewerSwIdx+1})`,
    `=SUM(C${fullSwIdx+1}:C${viewerSwIdx+1})`,
    `=SUM(D${fullSwIdx+1}:D${viewerSwIdx+1})`,
  ]);
  const totalSwitchSR = grid.length; // 1-based sheet row of Total Switch row

  addRow([]);
  addRow(["EXPANSION EFFECT  (revenue from new marketing employees signing up)"]);
  const slExpRevIdx  = grid.length;
  addRow(["Sales-Led new sign-ups",
    `=D${slPoolSR}*B${slExpSR}*B${mktgPriceSR}*12`,
    `=D${slPoolSR}*C${slExpSR}*B${mktgPriceSR}*12`,
    `=D${slPoolSR}*D${slExpSR}*B${mktgPriceSR}*12`,
  ]);
  const plgExpRevIdx = grid.length;
  addRow(["PLG new sign-ups (est.)",
    `=(ROUND(F${plgDataSR}/B${plgCoverageSR},0)-F${plgDataSR})*B${plgExpSR}*B${mktgPriceSR}*12`,
    `=(ROUND(F${plgDataSR}/C${plgCoverageSR},0)-F${plgDataSR})*C${plgExpSR}*B${mktgPriceSR}*12`,
    `=(ROUND(F${plgDataSR}/D${plgCoverageSR},0)-F${plgDataSR})*D${plgExpSR}*B${mktgPriceSR}*12`,
  ]);
  addRow(["Total Expansion Effect",
    `=SUM(B${slExpRevIdx+1}:B${plgExpRevIdx+1})`,
    `=SUM(C${slExpRevIdx+1}:C${plgExpRevIdx+1})`,
    `=SUM(D${slExpRevIdx+1}:D${plgExpRevIdx+1})`,
  ]);
  const totalExpSR = grid.length;

  addRow([]);
  addRow(["NET NEW ARR  (Switch Effect + Expansion Effect)",
    `=B${totalSwitchSR}+B${totalExpSR}`,
    `=C${totalSwitchSR}+C${totalExpSR}`,
    `=D${totalSwitchSR}+D${totalExpSR}`,
  ]);
  const netArrSR = grid.length;

  addRow([]);
  addRow(["VOLUME METRICS", "Conservative", "Base", "Aggressive"]);
  const switchUsersIdx = grid.length;
  addRow(["Mktg license users (from switches)",
    `=(B${plgDataSR}+B${slDataSR})*B${fullSwitchSR}+(C${plgDataSR}+C${slDataSR})*B${devSwitchSR}+(D${plgDataSR}+D${slDataSR})*B${collabSwitchSR}+(E${plgDataSR}+E${slDataSR})*B${viewerSwitchSR}`,
    `=(B${plgDataSR}+B${slDataSR})*C${fullSwitchSR}+(C${plgDataSR}+C${slDataSR})*C${devSwitchSR}+(D${plgDataSR}+D${slDataSR})*C${collabSwitchSR}+(E${plgDataSR}+E${slDataSR})*C${viewerSwitchSR}`,
    `=(B${plgDataSR}+B${slDataSR})*D${fullSwitchSR}+(C${plgDataSR}+C${slDataSR})*D${devSwitchSR}+(D${plgDataSR}+D${slDataSR})*D${collabSwitchSR}+(E${plgDataSR}+E${slDataSR})*D${viewerSwitchSR}`,
  ]);
  const expUsersIdx = grid.length;
  addRow(["Mktg license users (from expansion)",
    `=D${slPoolSR}*B${slExpSR}+(ROUND(F${plgDataSR}/B${plgCoverageSR},0)-F${plgDataSR})*B${plgExpSR}`,
    `=D${slPoolSR}*C${slExpSR}+(ROUND(F${plgDataSR}/C${plgCoverageSR},0)-F${plgDataSR})*C${plgExpSR}`,
    `=D${slPoolSR}*D${slExpSR}+(ROUND(F${plgDataSR}/D${plgCoverageSR},0)-F${plgDataSR})*D${plgExpSR}`,
  ]);
  const totalUsersIdx = grid.length;
  addRow(["Total Mktg License Users",
    `=B${switchUsersIdx+1}+B${expUsersIdx+1}`,
    `=C${switchUsersIdx+1}+C${expUsersIdx+1}`,
    `=D${switchUsersIdx+1}+D${expUsersIdx+1}`,
  ]);
  addRow(["Implied Annual Rev per User",
    `=IFERROR(B${netArrSR}/B${totalUsersIdx+1},0)`,
    `=IFERROR(C${netArrSR}/C${totalUsersIdx+1},0)`,
    `=IFERROR(D${netArrSR}/D${totalUsersIdx+1},0)`,
  ]);
  const impliedRevIdx = grid.length - 1;
  addRow([]);

  // ── SENSITIVITY TABLE ─────────────────────────────────────────────────
  // Rows = Marketing license price, Cols = SL expansion adoption rate
  // Fixed: Base switch rates, Base PLG coverage & expansion
  addRow(["SENSITIVITY: Net New ARR by Marketing License Price × SL Expansion Adoption Rate"]);
  addRow(["(Base switch rates applied; PLG expansion uses Base coverage ratio and adoption rate)"]);
  addRow([]);
  const sensHdrIdx = grid.length;
  addRow(["Price ↓  /  SL Adoption →", "5%", "10%", "15%", "20%", "25%"]);

  const PRICES    = [10, 15, 20, 25, 30];
  const ADOPTIONS = [0.05, 0.10, 0.15, 0.20, 0.25];
  const sensPriceStart = grid.length;

  for (const price of PRICES) {
    const row: FRow = [`$${price}/user/mo`];
    for (const adoption of ADOPTIONS) {
      const switchFml =
        `(B${plgDataSR}+B${slDataSR})*C${fullSwitchSR}*(${price}-B${fullPriceSR})*12` +
        `+(C${plgDataSR}+C${slDataSR})*C${devSwitchSR}*(${price}-B${devPriceSR})*12` +
        `+(D${plgDataSR}+D${slDataSR})*C${collabSwitchSR}*(${price}-B${collabPriceSR})*12` +
        `+(E${plgDataSR}+E${slDataSR})*C${viewerSwitchSR}*(${price}-B${viewerPriceSR})*12`;
      const slExpFml  = `D${slPoolSR}*${adoption}*${price}*12`;
      const plgExpFml = `(ROUND(F${plgDataSR}/C${plgCoverageSR},0)-F${plgDataSR})*C${plgExpSR}*${price}*12`;
      row.push(`=${switchFml}+${slExpFml}+${plgExpFml}`);
    }
    addRow(row);
  }
  const sensPriceEnd = grid.length;

  // ── Pad rows and write ────────────────────────────────────────────────
  const maxCols = 7;
  for (const row of grid) { while (row.length < maxCols) row.push(""); }
  console.log(`Writing ${grid.length} rows to Forecast sheet...`);
  await writeValues("Forecast!A1", grid);

  // ── Formatting ────────────────────────────────────────────────────────
  console.log("Applying Forecast formatting...");

  // Section headers (0-based grid rows)
  const sectionRows = [0, 2, 10, 17, 23, 29, 35];
  // Table headers (0-based)
  const tableHdrRows = [3, 11, 18, 24, 30, 36, 37, grid.length - (sensPriceEnd - sensHdrIdx) - 3, sensHdrIdx];

  // Infer the subheader rows from the known scenario section
  // "SWITCH EFFECT" subheader is just after main scenario header row 36
  const switchSubHdr  = 37; // grid row of "SWITCH EFFECT..." label
  const expSubHdr     = switchSubHdr + fullSwIdx - 37 + (viewerSwIdx - fullSwIdx) + 2; // computed below
  // Simple approach: identify by content — just use grid row indices we tracked
  const totalSwitchIdx = totalSwitchSR - 1; // 0-based
  const expSubHdrIdx   = totalSwitchIdx + 2; // blank + subheader
  const totalExpIdx    = totalExpSR - 1;
  const netArrIdx      = netArrSR - 1;
  const volHdrIdx      = netArrIdx + 2;

  const formatRequests: BatchRequest[] = [
    // Title
    ...sectionHeader(forecastSheetId, 0, 6),
    { repeatCell: {
        range: { sheetId: forecastSheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 6 },
        cell: { userEnteredFormat: { textFormat: { bold: true, fontSize: 16, foregroundColor: { red: 1, green: 1, blue: 1 } } } },
        fields: "userEnteredFormat.textFormat",
    } } satisfies RepeatCellRequest,

    // Section headers
    ...sectionRows.flatMap((r) => sectionHeader(forecastSheetId, r, 6)),

    // Table & sub-section headers
    ...tableHdrRows.flatMap((r) => tableHeader(forecastSheetId, r, 6)),
    // Subheaders within scenario section
    ...tableHeader(forecastSheetId, switchSubHdr, 4),
    ...tableHeader(forecastSheetId, expSubHdrIdx, 4),
    // NET NEW ARR — bold highlight
    boldRow(forecastSheetId, netArrIdx, 0, 4),
    bgRow(forecastSheetId, netArrIdx, 0, 4, 0.95, 0.98, 0.87),
    // Volume metrics header
    ...tableHeader(forecastSheetId, volHdrIdx, 4),

    // Column widths
    columnWidth(forecastSheetId, 0, 280),
    columnWidth(forecastSheetId, 1, 140),
    columnWidth(forecastSheetId, 2, 140),
    columnWidth(forecastSheetId, 3, 140),
    columnWidth(forecastSheetId, 4, 200),
    columnWidth(forecastSheetId, 5, 20),

    // Yellow cells (user assumption inputs)
    ...yellowCells.map(([sr, er, sc, ec]) => ({
      repeatCell: {
        range: { sheetId: forecastSheetId, startRowIndex: sr, endRowIndex: er, startColumnIndex: sc, endColumnIndex: ec },
        cell: { userEnteredFormat: { backgroundColor: { red: 1, green: 0.95, blue: 0.6 } } },
        fields: "userEnteredFormat.backgroundColor",
      },
    } satisfies RepeatCellRequest)),

    // % format for switch rates and expansion assumptions
    ...pctCells.map(([sr, er, sc, ec]) => numFmtRange(forecastSheetId, sr, er, sc, ec, "0%")),

    // $ format for price assumptions (col B = index 1)
    numFmtRange(forecastSheetId, priceHdrIdx, priceHdrIdx + 5, 1, 2, `$#,##0.00`),

    // #,##0 for current user counts and pool counts (cols B–F)
    numFmtRange(forecastSheetId, dataHdrIdx, dataHdrIdx + 3, 1, 6, "#,##0"),
    numFmtRange(forecastSheetId, poolHdrIdx, poolHdrIdx + 3, 1, 4, "#,##0"),

    // $#,##0 for all scenario revenue rows (cols B–D)
    numFmtRange(forecastSheetId, fullSwIdx, viewerSwIdx + 2, 1, 4, `$#,##0`), // switch rows + total
    numFmtRange(forecastSheetId, slExpRevIdx, plgExpRevIdx + 2, 1, 4, `$#,##0`), // expansion rows + total
    numFmtRange(forecastSheetId, netArrIdx, netArrIdx + 1, 1, 4, `$#,##0`), // net ARR

    // #,##0 for volume metrics (users)
    numFmtRange(forecastSheetId, switchUsersIdx, totalUsersIdx + 1, 1, 4, "#,##0"),
    // $#,##0 for implied ARR per user
    numFmtRange(forecastSheetId, impliedRevIdx, impliedRevIdx + 1, 1, 4, `$#,##0`),

    // $#,##0 for sensitivity table values (cols B–F)
    numFmtRange(forecastSheetId, sensPriceStart, sensPriceEnd, 1, 6, `$#,##0`),
  ];

  await batchUpdateSheet(formatRequests);
  console.log("✅ Forecast tab complete.");
}

// ── Marketing Tab ─────────────────────────────────────────────────────────
// PLG column map:  Tier=A Region=B Industry=C ARR=D FullSeats=E DevSeats=F
//                  CollabSeats=G ViewSeats=H MktgFull=I MktgDev=J
//                  MktgCollab=K MktgViewer=L BuzzMAU=M AvgAICost=N
// SL column map:   Id=A Tier=B Region=C Subregion=D Industry=E GrowthRate=F
//                  ARR=G Employees=H DesignerCount=I PMCount=J FEDevCount=K
//                  MktgCount=L OtherKW=M FullSeats=N DevSeats=O CollabSeats=P
//                  ViewSeats=Q MktgFull=R MktgDev=S MktgCollab=T MktgViewer=U
//                  BuzzMAU=V AvgAICost=W

async function buildMarketingTab(plgRows: string[][], slRows: string[][]): Promise<void> {
  console.log("\nBuilding Marketing tab...");

  // ── Sort-order aggregations (values come from sheet formulas; JS = ordering only)
  function aggMulti(rows: string[][], keyCol: number, valCols: number[]): Map<string, number> {
    const m = new Map<string, number>();
    for (const r of rows) {
      const k = r[keyCol] ?? "Unknown";
      m.set(k, (m.get(k) ?? 0) + valCols.reduce((s, c) => s + parseNum(r[c]), 0));
    }
    return m;
  }
  const plgMktgRegionOrder    = sortedDesc(aggMulti(plgRows, 1, [8, 9, 10, 11]));
  const plgMktgIndustryOrder  = sortedDesc(aggMulti(plgRows, 2, [8, 9, 10, 11]));
  const slMktgTierOrder       = sortedDesc(aggMulti(slRows, 1, [17, 18, 19, 20]));
  const slMktgRegionOrder     = sortedDesc(aggMulti(slRows, 2, [17, 18, 19, 20]));
  const slMktgIndustryOrder   = sortedDesc(aggMulti(slRows, 4, [17, 18, 19, 20])).slice(0, 10);

  const mktgSheetId = await getOrCreateSheet("Marketing", 1);

  // ── Formula string builders ────────────────────────────────────────────
  // plgM / plgS = PLG total marketing users / total seats by dimension
  const plgM = (dim: string, ri: number) =>
    `SUMIF(PLG!${dim},A${ri},PLG!I:I)+SUMIF(PLG!${dim},A${ri},PLG!J:J)+SUMIF(PLG!${dim},A${ri},PLG!K:K)+SUMIF(PLG!${dim},A${ri},PLG!L:L)`;
  const plgS = (dim: string, ri: number) =>
    `SUMIF(PLG!${dim},A${ri},PLG!E:E)+SUMIF(PLG!${dim},A${ri},PLG!F:F)+SUMIF(PLG!${dim},A${ri},PLG!G:G)+SUMIF(PLG!${dim},A${ri},PLG!H:H)`;
  // slM / slS = SL total marketing users / total seats by dimension
  const slM = (dim: string, ri: number) =>
    `SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!R:R)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!S:S)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!T:T)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!U:U)`;
  const slS = (dim: string, ri: number) =>
    `SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!N:N)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!O:O)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!P:P)+SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!Q:Q)`;
  // slMktgEmp = SL marketing employee headcount at company by dimension
  const slMktgEmp = (dim: string, ri: number) =>
    `SUMIF('Sales-Led'!${dim},A${ri},'Sales-Led'!L:L)`;

  // ── Grid construction ────────────────────────────────────────────────────
  type MktgRow = (string | number)[];
  const grid: MktgRow[] = [];
  // Track which 0-based row/col ranges need % number formatting
  const pctRanges: Array<[number, number, number, number]> = [];

  function addRow(cols: MktgRow = []): number {
    grid.push(cols);
    return grid.length - 1;
  }
  // 1-based sheet row for the row that is about to be added
  function nextSR(): number { return grid.length + 1; }

  // Title
  addRow(["MARKETING USERS ANALYSIS"]);
  addRow([]);

  // ── KPI Summary ─────────────────────────────────────────────────────────
  const kpiStart = grid.length;
  const kpiMktgSR  = kpiStart + 2; // 1-based sheet row of "Total Marketing Users"
  const kpiSeatsSR = kpiStart + 3; // 1-based sheet row of "Total All Seats"

  addRow(["METRIC", "PLG", "SALES-LED", "COMBINED"]);
  // Total Marketing Users
  addRow(["Total Marketing Users",
    "=SUM(PLG!I:I)+SUM(PLG!J:J)+SUM(PLG!K:K)+SUM(PLG!L:L)",
    "=SUM('Sales-Led'!R:R)+SUM('Sales-Led'!S:S)+SUM('Sales-Led'!T:T)+SUM('Sales-Led'!U:U)",
    `=B${kpiMktgSR}+C${kpiMktgSR}`,
  ]);
  // Total All Seats
  addRow(["Total All Seats",
    "=SUM(PLG!E:E)+SUM(PLG!F:F)+SUM(PLG!G:G)+SUM(PLG!H:H)",
    "=SUM('Sales-Led'!N:N)+SUM('Sales-Led'!O:O)+SUM('Sales-Led'!P:P)+SUM('Sales-Led'!Q:Q)",
    `=B${kpiSeatsSR}+C${kpiSeatsSR}`,
  ]);
  // Marketing % of All Seats (percentage row)
  addRow(["Marketing % of All Seats",
    `=IFERROR(B${kpiMktgSR}/B${kpiSeatsSR},0)`,
    `=IFERROR(C${kpiMktgSR}/C${kpiSeatsSR},0)`,
    `=IFERROR(D${kpiMktgSR}/D${kpiSeatsSR},0)`,
  ]);
  pctRanges.push([kpiStart + 3, kpiStart + 4, 1, 4]);
  // Breakdown by seat type
  for (const [label, plgCol, slCol] of [
    ["Mktg Users on Full Seats",   "PLG!I:I", "'Sales-Led'!R:R"],
    ["Mktg Users on Dev Seats",    "PLG!J:J", "'Sales-Led'!S:S"],
    ["Mktg Users on Collab Seats", "PLG!K:K", "'Sales-Led'!T:T"],
    ["Mktg Users on Viewer Seats", "PLG!L:L", "'Sales-Led'!U:U"],
  ] as [string, string, string][]) {
    const r = nextSR();
    addRow([label, `=SUM(${plgCol})`, `=SUM(${slCol})`, `=B${r}+C${r}`]);
  }
  addRow([]);

  // ── Section: PLG — Marketing by Seat Type ────────────────────────────────
  addRow(["PLG: Marketing Users by Seat Type"]);
  const plgSeatHdr = grid.length;
  addRow(["Seat Type", "Total Seats", "Mktg Users", "Mktg %"]);
  for (const [label, seatCol, mktgCol] of [
    ["Full Seats",   "PLG!E:E", "PLG!I:I"],
    ["Dev Seats",    "PLG!F:F", "PLG!J:J"],
    ["Collab Seats", "PLG!G:G", "PLG!K:K"],
    ["View Seats",   "PLG!H:H", "PLG!L:L"],
  ] as [string, string, string][]) {
    const r = nextSR();
    addRow([label, `=SUM(${seatCol})`, `=SUM(${mktgCol})`, `=IFERROR(C${r}/B${r},0)`]);
  }
  pctRanges.push([plgSeatHdr + 1, plgSeatHdr + 5, 3, 4]);
  addRow([]);

  // ── Section: SL — Marketing by Seat Type ─────────────────────────────────
  addRow(["SL: Marketing Users by Seat Type"]);
  const slSeatHdr = grid.length;
  addRow(["Seat Type", "Total Seats", "Mktg Users", "Mktg %"]);
  for (const [label, seatCol, mktgCol] of [
    ["Full Seats",   "'Sales-Led'!N:N", "'Sales-Led'!R:R"],
    ["Dev Seats",    "'Sales-Led'!O:O", "'Sales-Led'!S:S"],
    ["Collab Seats", "'Sales-Led'!P:P", "'Sales-Led'!T:T"],
    ["View Seats",   "'Sales-Led'!Q:Q", "'Sales-Led'!U:U"],
  ] as [string, string, string][]) {
    const r = nextSR();
    addRow([label, `=SUM(${seatCol})`, `=SUM(${mktgCol})`, `=IFERROR(C${r}/B${r},0)`]);
  }
  pctRanges.push([slSeatHdr + 1, slSeatHdr + 5, 3, 4]);
  addRow([]);

  // ── Section: PLG — Marketing % by Region ─────────────────────────────────
  addRow(["PLG: Marketing % by Region"]);
  const plgMktgRegHdr = grid.length;
  addRow(["Region", "Mktg Users", "Total Seats", "Mktg %", "Accounts"]);
  for (const [k] of plgMktgRegionOrder) {
    const r = nextSR();
    addRow([k, `=${plgM("B:B", r)}`, `=${plgS("B:B", r)}`, `=IFERROR(B${r}/C${r},0)`, `=COUNTIF(PLG!B:B,A${r})`]);
  }
  const plgMktgRegCount = plgMktgRegionOrder.length;
  pctRanges.push([plgMktgRegHdr + 1, plgMktgRegHdr + 1 + plgMktgRegCount, 3, 4]);
  addRow([]);

  // ── Section: SL — Marketing % by Region ──────────────────────────────────
  addRow(["SL: Marketing % by Region"]);
  const slMktgRegHdr = grid.length;
  addRow(["Region", "Mktg Users", "Total Seats", "Mktg %", "Accounts"]);
  for (const [k] of slMktgRegionOrder) {
    const r = nextSR();
    addRow([k, `=${slM("C:C", r)}`, `=${slS("C:C", r)}`, `=IFERROR(B${r}/C${r},0)`, `=COUNTIF('Sales-Led'!C:C,A${r})`]);
  }
  const slMktgRegCount = slMktgRegionOrder.length;
  pctRanges.push([slMktgRegHdr + 1, slMktgRegHdr + 1 + slMktgRegCount, 3, 4]);
  addRow([]);

  // ── Section: PLG — Marketing % by Industry ───────────────────────────────
  addRow(["PLG: Marketing % by Industry"]);
  const plgMktgIndHdr = grid.length;
  addRow(["Industry Group", "Mktg Users", "Total Seats", "Mktg %", "Accounts"]);
  for (const [k] of plgMktgIndustryOrder) {
    const r = nextSR();
    addRow([k, `=${plgM("C:C", r)}`, `=${plgS("C:C", r)}`, `=IFERROR(B${r}/C${r},0)`, `=COUNTIF(PLG!C:C,A${r})`]);
  }
  const plgMktgIndCount = plgMktgIndustryOrder.length;
  pctRanges.push([plgMktgIndHdr + 1, plgMktgIndHdr + 1 + plgMktgIndCount, 3, 4]);
  addRow([]);

  // ── Section: SL — Top 10 Industries by Marketing Users ───────────────────
  addRow(["SL: Top 10 Industries by Marketing Users"]);
  const slMktgIndHdr = grid.length;
  addRow(["Industry Group", "Mktg Users", "Total Seats", "Mktg %", "Accounts"]);
  for (const [k] of slMktgIndustryOrder) {
    const r = nextSR();
    addRow([k, `=${slM("E:E", r)}`, `=${slS("E:E", r)}`, `=IFERROR(B${r}/C${r},0)`, `=COUNTIF('Sales-Led'!E:E,A${r})`]);
  }
  const slMktgIndCount = slMktgIndustryOrder.length;
  pctRanges.push([slMktgIndHdr + 1, slMktgIndHdr + 1 + slMktgIndCount, 3, 4]);
  addRow([]);

  // ── Section: SL — Adoption Rate by Tier ──────────────────────────────────
  // Adoption = marketing users in product ÷ marketing employees at company
  addRow(["SL: Marketing Adoption Rate by Tier"]);
  const slAdoptTierHdr = grid.length;
  addRow(["Tier", "Mktg Users (Product)", "Mktg Employees (Company)", "Adoption Rate", "Accounts"]);
  for (const [k] of slMktgTierOrder) {
    const r = nextSR();
    addRow([k,
      `=${slM("B:B", r)}`,
      `=${slMktgEmp("B:B", r)}`,
      `=IFERROR(B${r}/C${r},0)`,
      `=COUNTIF('Sales-Led'!B:B,A${r})`,
    ]);
  }
  const slAdoptTierCount = slMktgTierOrder.length;
  pctRanges.push([slAdoptTierHdr + 1, slAdoptTierHdr + 1 + slAdoptTierCount, 3, 4]);
  addRow([]);

  // ── Section: SL — Adoption Rate by Region ────────────────────────────────
  addRow(["SL: Marketing Adoption Rate by Region"]);
  const slAdoptRegHdr = grid.length;
  addRow(["Region", "Mktg Users (Product)", "Mktg Employees (Company)", "Adoption Rate", "Accounts"]);
  for (const [k] of slMktgRegionOrder) {
    const r = nextSR();
    addRow([k,
      `=${slM("C:C", r)}`,
      `=${slMktgEmp("C:C", r)}`,
      `=IFERROR(B${r}/C${r},0)`,
      `=COUNTIF('Sales-Led'!C:C,A${r})`,
    ]);
  }
  const slAdoptRegCount = slMktgRegionOrder.length;
  pctRanges.push([slAdoptRegHdr + 1, slAdoptRegHdr + 1 + slAdoptRegCount, 3, 4]);
  addRow([]);

  // ── Section: SL — Adoption Rate by Industry (top 10) ─────────────────────
  addRow(["SL: Marketing Adoption Rate by Industry (Top 10)"]);
  const slAdoptIndHdr = grid.length;
  addRow(["Industry Group", "Mktg Users (Product)", "Mktg Employees (Company)", "Adoption Rate", "Accounts"]);
  for (const [k] of slMktgIndustryOrder) {
    const r = nextSR();
    addRow([k,
      `=${slM("E:E", r)}`,
      `=${slMktgEmp("E:E", r)}`,
      `=IFERROR(B${r}/C${r},0)`,
      `=COUNTIF('Sales-Led'!E:E,A${r})`,
    ]);
  }
  const slAdoptIndCount = slMktgIndustryOrder.length;
  pctRanges.push([slAdoptIndHdr + 1, slAdoptIndHdr + 1 + slAdoptIndCount, 3, 4]);

  // ── Pad and write ─────────────────────────────────────────────────────────
  const maxCols = 7;
  for (const row of grid) { while (row.length < maxCols) row.push(""); }
  console.log(`Writing ${grid.length} rows to Marketing sheet...`);
  await writeValues("Marketing!A1", grid);

  // ── Formatting ────────────────────────────────────────────────────────────
  console.log("Applying Marketing formatting...");

  const sectionRowsList = [
    plgSeatHdr - 1, slSeatHdr - 1,
    plgMktgRegHdr - 1, slMktgRegHdr - 1,
    plgMktgIndHdr - 1, slMktgIndHdr - 1,
    slAdoptTierHdr - 1, slAdoptRegHdr - 1, slAdoptIndHdr - 1,
  ];
  const tableHeaderRowsList = [
    plgSeatHdr, slSeatHdr,
    plgMktgRegHdr, slMktgRegHdr,
    plgMktgIndHdr, slMktgIndHdr,
    slAdoptTierHdr, slAdoptRegHdr, slAdoptIndHdr,
  ];

  const formatRequests: BatchRequest[] = [
    // Title row
    ...sectionHeader(mktgSheetId, 0, 5),
    {
      repeatCell: {
        range: { sheetId: mktgSheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 5 },
        cell: { userEnteredFormat: { textFormat: { bold: true, fontSize: 16, foregroundColor: { red: 1, green: 1, blue: 1 } } } },
        fields: "userEnteredFormat.textFormat",
      },
    } satisfies RepeatCellRequest,
    // KPI header row
    bgRow(mktgSheetId, kpiStart, 0, 4, 0.22, 0.44, 0.78),
    boldRow(mktgSheetId, kpiStart, 0, 4),
    {
      repeatCell: {
        range: { sheetId: mktgSheetId, startRowIndex: kpiStart, endRowIndex: kpiStart + 1, startColumnIndex: 0, endColumnIndex: 4 },
        cell: { userEnteredFormat: { textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } } } },
        fields: "userEnteredFormat.textFormat",
      },
    } satisfies RepeatCellRequest,
    // Section and table headers
    ...sectionRowsList.flatMap((r) => sectionHeader(mktgSheetId, r, 5)),
    ...tableHeaderRowsList.flatMap((r) => tableHeader(mktgSheetId, r, 5)),
    // Column widths
    columnWidth(mktgSheetId, 0, 250),
    columnWidth(mktgSheetId, 1, 155),
    columnWidth(mktgSheetId, 2, 190),
    columnWidth(mktgSheetId, 3, 100),
    columnWidth(mktgSheetId, 4, 90),
    columnWidth(mktgSheetId, 5, 20),
    columnWidth(mktgSheetId, 6, 530),
    // KPI: count rows (B–D)
    numFmtRange(mktgSheetId, kpiStart + 1, kpiStart + 3, 1, 4, "#,##0"), // Mktg Users, All Seats
    numFmtRange(mktgSheetId, kpiStart + 4, kpiStart + 8, 1, 4, "#,##0"), // per-seat-type rows
    // Seat tables: B (total seats) and C (mktg users) → #,##0
    numFmtRange(mktgSheetId, plgSeatHdr + 1, plgSeatHdr + 5, 1, 3, "#,##0"),
    numFmtRange(mktgSheetId, slSeatHdr + 1,  slSeatHdr + 5,  1, 3, "#,##0"),
    // Region / industry tables: B (mktg users) and C (total seats) → #,##0
    numFmtRange(mktgSheetId, plgMktgRegHdr + 1, plgMktgRegHdr + 1 + plgMktgRegCount, 1, 3, "#,##0"),
    numFmtRange(mktgSheetId, slMktgRegHdr + 1,  slMktgRegHdr  + 1 + slMktgRegCount,  1, 3, "#,##0"),
    numFmtRange(mktgSheetId, plgMktgIndHdr + 1, plgMktgIndHdr + 1 + plgMktgIndCount, 1, 3, "#,##0"),
    numFmtRange(mktgSheetId, slMktgIndHdr + 1,  slMktgIndHdr  + 1 + slMktgIndCount,  1, 3, "#,##0"),
    // Region / industry accounts column → #,##0
    numFmtRange(mktgSheetId, plgMktgRegHdr + 1, plgMktgRegHdr + 1 + plgMktgRegCount, 4, 5, "#,##0"),
    numFmtRange(mktgSheetId, slMktgRegHdr + 1,  slMktgRegHdr  + 1 + slMktgRegCount,  4, 5, "#,##0"),
    numFmtRange(mktgSheetId, plgMktgIndHdr + 1, plgMktgIndHdr + 1 + plgMktgIndCount, 4, 5, "#,##0"),
    numFmtRange(mktgSheetId, slMktgIndHdr + 1,  slMktgIndHdr  + 1 + slMktgIndCount,  4, 5, "#,##0"),
    // Adoption tables: B (mktg users) and C (mktg employees) → #,##0
    numFmtRange(mktgSheetId, slAdoptTierHdr + 1, slAdoptTierHdr + 1 + slAdoptTierCount, 1, 3, "#,##0"),
    numFmtRange(mktgSheetId, slAdoptRegHdr + 1,  slAdoptRegHdr  + 1 + slAdoptRegCount,  1, 3, "#,##0"),
    numFmtRange(mktgSheetId, slAdoptIndHdr + 1,  slAdoptIndHdr  + 1 + slAdoptIndCount,  1, 3, "#,##0"),
    // Adoption accounts column → #,##0
    numFmtRange(mktgSheetId, slAdoptTierHdr + 1, slAdoptTierHdr + 1 + slAdoptTierCount, 4, 5, "#,##0"),
    numFmtRange(mktgSheetId, slAdoptRegHdr + 1,  slAdoptRegHdr  + 1 + slAdoptRegCount,  4, 5, "#,##0"),
    numFmtRange(mktgSheetId, slAdoptIndHdr + 1,  slAdoptIndHdr  + 1 + slAdoptIndCount,  4, 5, "#,##0"),
    // Percentage columns (all pctRanges tracked above → "0.00%")
    ...pctRanges.map(([sr, er, sc, ec]) => numFmtRange(mktgSheetId, sr, er, sc, ec, "0.00%")),
  ];
  await batchUpdateSheet(formatRequests);

  // ── Charts ────────────────────────────────────────────────────────────────
  console.log("Adding Marketing charts...");
  const MKTG_COL = 6;
  let mktgChartRow = 1;
  function nextMktgAnchor(h: number): number {
    const a = mktgChartRow;
    mktgChartRow += Math.ceil(h / 21) + 2;
    return a;
  }

  const chartReqs: AddChartRequest[] = [
    // 1. PLG marketing users by seat type (pie)
    pieChart("PLG: Marketing Users by Seat Type", {
      sheetId: mktgSheetId, headerRow: plgSeatHdr, dataRows: 4,
      catCol: 0, valCol: 2,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 440, heightPx: 280,
    }),
    // 2. SL marketing users by seat type (pie)
    pieChart("SL: Marketing Users by Seat Type", {
      sheetId: mktgSheetId, headerRow: slSeatHdr, dataRows: 4,
      catCol: 0, valCol: 2,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 440, heightPx: 280,
    }),
    // 3. PLG marketing users by region (bar)
    barChart("PLG: Marketing Users by Region", {
      sheetId: mktgSheetId, headerRow: plgMktgRegHdr, dataRows: plgMktgRegCount,
      catCol: 0, valCol: 1,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 480, heightPx: 280,
    }),
    // 4. SL marketing users by region (bar)
    barChart("SL: Marketing Users by Region", {
      sheetId: mktgSheetId, headerRow: slMktgRegHdr, dataRows: slMktgRegCount,
      catCol: 0, valCol: 1,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 480, heightPx: 280,
    }),
    // 5. PLG marketing users by industry (bar)
    barChart("PLG: Marketing Users by Industry", {
      sheetId: mktgSheetId, headerRow: plgMktgIndHdr, dataRows: plgMktgIndCount,
      catCol: 0, valCol: 1,
      anchorRow: nextMktgAnchor(380), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 500, heightPx: 380,
    }),
    // 6. SL top 10 industries by marketing users (bar)
    barChart("SL: Top 10 Industries by Marketing Users", {
      sheetId: mktgSheetId, headerRow: slMktgIndHdr, dataRows: slMktgIndCount,
      catCol: 0, valCol: 1,
      anchorRow: nextMktgAnchor(380), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 500, heightPx: 380,
    }),
    // 7. SL adoption rate by tier (column) — Y-axis 0–1 = 0%–100%
    colChart("SL: Marketing Adoption Rate by Tier", {
      sheetId: mktgSheetId, headerRow: slAdoptTierHdr, dataRows: slAdoptTierCount,
      catCol: 0, valCol: 3,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 480, heightPx: 280,
    }),
    // 8. SL adoption rate by region (bar)
    barChart("SL: Marketing Adoption Rate by Region", {
      sheetId: mktgSheetId, headerRow: slAdoptRegHdr, dataRows: slAdoptRegCount,
      catCol: 0, valCol: 3,
      anchorRow: nextMktgAnchor(280), anchorCol: MKTG_COL,
      chartSheetId: mktgSheetId, widthPx: 480, heightPx: 280,
    }),
  ];
  await batchUpdateSheet(chartReqs);
  console.log("✅ Marketing tab complete.");
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("Reading PLG data...");
  const plgRaw = await getValues("PLG");
  const plgRows = plgRaw.slice(1);

  console.log("Reading Sales-Led data...");
  const slRaw = await getValues("Sales-Led");
  const slRows = slRaw.slice(1);

  // ── PLG Aggregations ──────────────────────────────────────────────────
  // Cols: Tier(0) Region(1) Industry(2) ARR(3) FullSeats(4) DevSeats(5)
  //       CollabSeats(6) ViewSeats(7) … BuzzMAU(12) AvgAICost(13)

  const plgByRegion   = aggregateBy(plgRows, 1, 3);
  const plgByIndustry = aggregateBy(plgRows, 2, 3);

  const plgTotalARR    = [...plgByRegion.values()].reduce((s, v) => s + v, 0);
  const plgTotalBuzzMAU = plgRows.reduce((s, r) => s + parseNum(r[12]), 0);
  const plgAvgAICost   = plgRows.length > 0
    ? plgRows.reduce((s, r) => s + parseNum(r[13]), 0) / plgRows.length
    : 0;
  let plgFullSeats = 0, plgDevSeats = 0, plgCollabSeats = 0, plgViewSeats = 0;
  for (const r of plgRows) {
    plgFullSeats   += parseNum(r[4]);
    plgDevSeats    += parseNum(r[5]);
    plgCollabSeats += parseNum(r[6]);
    plgViewSeats   += parseNum(r[7]);
  }

  const plgRegionRows   = sortedDesc(plgByRegion);
  const plgIndustryRows = sortedDesc(plgByIndustry);

  // ── Sales-Led Aggregations ────────────────────────────────────────────
  // Cols: Id(0) Tier(1) Region(2) Subregion(3) Industry(4) GrowthRate(5)
  //       ARR(6) Employees(7) … FullSeats(13) DevSeats(14) CollabSeats(15)
  //       ViewSeats(16) … BuzzMAU(21) AvgAICost(22)

  const slTierCounts    = new Map<string, number>();
  const slRegionCounts  = new Map<string, number>();
  const slIndustryCounts = new Map<string, number>();

  const slByTier     = aggregateBy(slRows, 1, 6, slTierCounts);
  const slByRegion   = aggregateBy(slRows, 2, 6, slRegionCounts);
  const slByIndustry = aggregateBy(slRows, 4, 6, slIndustryCounts);

  const slTotalARR     = [...slByTier.values()].reduce((s, v) => s + v, 0);
  const slTotalBuzzMAU = slRows.reduce((s, r) => s + parseNum(r[21]), 0);
  const slAvgAICost    = slRows.length > 0
    ? slRows.reduce((s, r) => s + parseNum(r[22]), 0) / slRows.length
    : 0;
  let slFullSeats = 0, slDevSeats = 0, slCollabSeats = 0, slViewSeats = 0;
  for (const r of slRows) {
    slFullSeats   += parseNum(r[13]);
    slDevSeats    += parseNum(r[14]);
    slCollabSeats += parseNum(r[15]);
    slViewSeats   += parseNum(r[16]);
  }

  const slTierRows     = sortedDesc(slByTier);
  const slRegionRows   = sortedDesc(slByRegion);
  const slIndustryRows = sortedDesc(slByIndustry).slice(0, 10);

  // Avg AI Cost per MAU by region
  const slAICostByRegion = new Map<string, { total: number; count: number }>();
  for (const r of slRows) {
    const region = r[2] ?? "Unknown";
    const cost = parseNum(r[22]);
    const e = slAICostByRegion.get(region) ?? { total: 0, count: 0 };
    e.total += cost; e.count += 1;
    slAICostByRegion.set(region, e);
  }
  const slAvgCostByRegion = sortedDesc(
    new Map([...slAICostByRegion.entries()].map(([k, v]) => [k, v.total / v.count]))
  );

  console.log(`PLG total ARR: ${fmt(plgTotalARR)} across ${plgRows.length} accounts`);
  console.log(`SL total ARR:  ${fmt(slTotalARR)} across ${slRows.length} accounts`);

  // ── Create / clear Insights sheet ────────────────────────────────────
  const insightsSheetId = await getOrCreateSheet("Insights", 0);

  // ── Build table data with tracked row positions ────────────────────────

  type GridRow = (string | number)[];
  const grid: GridRow[] = [];

  function addRow(cols: GridRow = []): number {
    const row = grid.length;
    grid.push(cols);
    return row;
  }

  // Row 0: Title
  addRow(["REPLIT TINKERING — INSIGHTS DASHBOARD"]);
  addRow([]); // blank

  // KPI block occupies cols 4-6 starting at row 2
  const kpiStartRow = grid.length;

  // ── PLG section ───────────────────────────────────────────────────────
  // Category labels come from JS (for sort order); values are live sheet formulas.
  // PLG columns: Tier=A Region=B Industry=C ARR=D FullSeats=E DevSeats=F
  //              CollabSeats=G ViewSeats=H … BuzzMAU=M AvgAICost=N

  addRow(["PLG: ARR by Region"]);
  const plgRegionHeaderRow = grid.length;
  addRow(["Region", "ARR ($)"]);
  for (const [k] of plgRegionRows) {
    const ri = grid.length + 1;
    addRow([k, `=SUMIF(PLG!B:B,A${ri},PLG!D:D)`]);
  }
  const plgRegionDataCount = plgRegionRows.length;

  addRow([]);

  addRow(["PLG: ARR by Industry Group"]);
  const plgIndustryHeaderRow = grid.length;
  addRow(["Industry Group", "ARR ($)"]);
  for (const [k] of plgIndustryRows) {
    const ri = grid.length + 1;
    addRow([k, `=SUMIF(PLG!C:C,A${ri},PLG!D:D)`]);
  }
  const plgIndustryDataCount = plgIndustryRows.length;

  addRow([]);

  addRow(["PLG: Seat Type Distribution"]);
  const plgSeatHeaderRow = grid.length;
  addRow(["Seat Type", "Total Seats"]);
  addRow(["Full Seats",   "=SUM(PLG!E:E)"]);
  addRow(["Dev Seats",    "=SUM(PLG!F:F)"]);
  addRow(["Collab Seats", "=SUM(PLG!G:G)"]);
  addRow(["View Seats",   "=SUM(PLG!H:H)"]);

  addRow([]); addRow([]);

  // ── Sales-Led section ─────────────────────────────────────────────────
  // Sales-Led columns: Id=A Tier=B Region=C Subregion=D Industry=E GrowthRate=F
  //                    ARR=G … FullSeats=N DevSeats=O CollabSeats=P ViewSeats=Q
  //                    … BuzzMAU=V AvgAICost=W

  addRow(["SALES-LED: ARR by Tier"]);
  const slTierHeaderRow = grid.length;
  addRow(["Tier", "ARR ($)", "Accounts", "Avg ARR ($)"]);
  for (const [k] of slTierRows) {
    const ri = grid.length + 1;
    addRow([k,
      `=SUMIF('Sales-Led'!B:B,A${ri},'Sales-Led'!G:G)`,
      `=COUNTIF('Sales-Led'!B:B,A${ri})`,
      `=IFERROR(B${ri}/C${ri},0)`,
    ]);
  }
  const slTierDataCount = slTierRows.length;

  addRow([]);

  addRow(["SALES-LED: ARR by Region"]);
  const slRegionHeaderRow = grid.length;
  addRow(["Region", "ARR ($)", "Accounts"]);
  for (const [k] of slRegionRows) {
    const ri = grid.length + 1;
    addRow([k,
      `=SUMIF('Sales-Led'!C:C,A${ri},'Sales-Led'!G:G)`,
      `=COUNTIF('Sales-Led'!C:C,A${ri})`,
    ]);
  }
  const slRegionDataCount = slRegionRows.length;

  addRow([]);

  addRow(["SALES-LED: Top 10 Industries by ARR"]);
  const slIndustryHeaderRow = grid.length;
  addRow(["Industry Group", "ARR ($)", "Accounts"]);
  for (const [k] of slIndustryRows) {
    const ri = grid.length + 1;
    addRow([k,
      `=SUMIF('Sales-Led'!E:E,A${ri},'Sales-Led'!G:G)`,
      `=COUNTIF('Sales-Led'!E:E,A${ri})`,
    ]);
  }
  const slIndustryDataCount = slIndustryRows.length;

  addRow([]);

  addRow(["SALES-LED: Seat Type Distribution"]);
  const slSeatHeaderRow = grid.length;
  addRow(["Seat Type", "Total Seats"]);
  addRow(["Full Seats",   "=SUM('Sales-Led'!N:N)"]);
  addRow(["Dev Seats",    "=SUM('Sales-Led'!O:O)"]);
  addRow(["Collab Seats", "=SUM('Sales-Led'!P:P)"]);
  addRow(["View Seats",   "=SUM('Sales-Led'!Q:Q)"]);

  addRow([]);

  addRow(["SALES-LED: Avg AI Cost/MAU by Region"]);
  const slAICostHeaderRow = grid.length;
  addRow(["Region", "Avg AI Cost/MAU ($)"]);
  for (const [k] of slAvgCostByRegion) {
    const ri = grid.length + 1;
    addRow([k, `=AVERAGEIF('Sales-Led'!C:C,A${ri},'Sales-Led'!W:W)`]);
  }
  const slAICostDataCount = slAvgCostByRegion.length;

  addRow([]); addRow([]);

  // ── Growth Rate vs ARR trend (Sales-Led) ──────────────────────────────
  // Read unique growth rate labels from data (for row ordering only).
  // The actual ARR sums are computed by SUMIF formulas in the sheet.
  const growthRateARR = new Map<string, { arr: number; count: number }>();
  for (const r of slRows) {
    const rate = (r[5] ?? "0%").replace("%", "").trim();
    const rateLabel = rate + "%";
    const arr = parseNum(r[6]);
    const e = growthRateARR.get(rateLabel) ?? { arr: 0, count: 0 };
    e.arr += arr; e.count += 1;
    growthRateARR.set(rateLabel, e);
  }
  const growthTrendRows = [...growthRateARR.entries()]
    .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]));

  addRow(["SALES-LED: ARR by Industry Growth Rate (trend)"]);
  const growthTrendHeaderRow = grid.length;
  addRow(["Growth Rate", "Total ARR ($)", "Accounts"]);
  for (const [rate] of growthTrendRows) {
    const ri = grid.length + 1;
    addRow([rate,
      `=SUMIF('Sales-Led'!F:F,A${ri},'Sales-Led'!G:G)`,
      `=COUNTIF('Sales-Led'!F:F,A${ri})`,
    ]);
  }
  const growthTrendDataCount = growthTrendRows.length;

  addRow([]);

  // ── PLG vs Sales-Led: Seat Mix Comparison ─────────────────────────────
  addRow(["PLG vs Sales-Led: Seat Mix Comparison"]);
  const seatCompHeaderRow = grid.length;
  addRow(["Seat Type", "PLG Seats", "Sales-Led Seats"]);
  addRow(["Full Seats",   "=SUM(PLG!E:E)", "=SUM('Sales-Led'!N:N)"]);
  addRow(["Dev Seats",    "=SUM(PLG!F:F)", "=SUM('Sales-Led'!O:O)"]);
  addRow(["Collab Seats", "=SUM(PLG!G:G)", "=SUM('Sales-Led'!P:P)"]);
  addRow(["View Seats",   "=SUM(PLG!H:H)", "=SUM('Sales-Led'!Q:Q)"]);
  const seatCompDataCount = 4;

  // ── Inject KPI block into cols 4-6 ───────────────────────────────────

  const kpiData: GridRow[] = [
    ["KEY METRICS",        "PLG",                                                          "Sales-Led"],
    ["Total ARR",          "=SUM(PLG!D:D)",                                               "=SUM('Sales-Led'!G:G)"],
    ["Accounts",           "=COUNTA(PLG!A:A)-1",                                          "=COUNTA('Sales-Led'!A:A)-1"],
    ["Avg ARR / Account",  "=IFERROR(SUM(PLG!D:D)/(COUNTA(PLG!A:A)-1),0)",               "=IFERROR(SUM('Sales-Led'!G:G)/(COUNTA('Sales-Led'!A:A)-1),0)"],
    ["Total Buzz MAU",     "=SUM(PLG!M:M)",                                               "=SUM('Sales-Led'!V:V)"],
    ["Avg AI Cost / MAU",  "=IFERROR(AVERAGE(PLG!N:N),0)",                               "=IFERROR(AVERAGE('Sales-Led'!W:W),0)"],
  ];
  for (let i = 0; i < kpiData.length; i++) {
    const rowIdx = kpiStartRow + i;
    while (grid.length <= rowIdx) grid.push([]);
    const row = grid[rowIdx];
    while (row.length < 7) row.push("");
    row[4] = kpiData[i][0];
    row[5] = kpiData[i][1];
    row[6] = kpiData[i][2];
  }

  // ── Pad all rows to consistent width ─────────────────────────────────
  const maxCols = 7;
  for (const row of grid) {
    while (row.length < maxCols) row.push("");
  }

  // ── Write data ────────────────────────────────────────────────────────
  console.log(`Writing ${grid.length} rows to Insights sheet...`);
  await writeValues("Insights!A1", grid);

  // ── Formatting ────────────────────────────────────────────────────────
  console.log("Applying formatting...");

  const sectionRows = [
    plgRegionHeaderRow - 1,
    plgIndustryHeaderRow - 1,
    plgSeatHeaderRow - 1,
    slTierHeaderRow - 1,
    slRegionHeaderRow - 1,
    slIndustryHeaderRow - 1,
    slSeatHeaderRow - 1,
    slAICostHeaderRow - 1,
    growthTrendHeaderRow - 1,
    seatCompHeaderRow - 1,
  ];

  const tableHeaderRows = [
    plgRegionHeaderRow,
    plgIndustryHeaderRow,
    plgSeatHeaderRow,
    slTierHeaderRow,
    slRegionHeaderRow,
    slIndustryHeaderRow,
    slSeatHeaderRow,
    slAICostHeaderRow,
    growthTrendHeaderRow,
    seatCompHeaderRow,
  ];

  const kpiHeaderFormat: RepeatCellRequest = {
    repeatCell: {
      range: { sheetId: insightsSheetId, startRowIndex: kpiStartRow, endRowIndex: kpiStartRow + 1, startColumnIndex: 4, endColumnIndex: 7 },
      cell: { userEnteredFormat: { textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } } } },
      fields: "userEnteredFormat.textFormat",
    },
  };

  const formatRequests: BatchRequest[] = [
    // Title row
    ...sectionHeader(insightsSheetId, 0, 4),
    {
      repeatCell: {
        range: { sheetId: insightsSheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 4 },
        cell: { userEnteredFormat: { textFormat: { bold: true, fontSize: 16, foregroundColor: { red: 1, green: 1, blue: 1 } } } },
        fields: "userEnteredFormat.textFormat",
      },
    } satisfies RepeatCellRequest,
    // Section headers
    ...sectionRows.flatMap((r) => sectionHeader(insightsSheetId, r, 4)),
    // Table column headers
    ...tableHeaderRows.flatMap((r) => tableHeader(insightsSheetId, r, 4)),
    // KPI header
    bgRow(insightsSheetId, kpiStartRow, 4, 7, 0.22, 0.44, 0.78),
    boldRow(insightsSheetId, kpiStartRow, 4, 7),
    kpiHeaderFormat,
    // Column widths
    columnWidth(insightsSheetId, 0, 220),
    columnWidth(insightsSheetId, 1, 130),
    columnWidth(insightsSheetId, 2, 100),
    columnWidth(insightsSheetId, 3, 20),
    columnWidth(insightsSheetId, 4, 180),
    columnWidth(insightsSheetId, 5, 110),
    columnWidth(insightsSheetId, 6, 110),
    columnWidth(insightsSheetId, 7, 20),
    columnWidth(insightsSheetId, 8, 540),

    // ── Number formatting ─────────────────────────────────────────────────
    // Dollar columns (col B = index 1): $#,##0
    numFmtRange(insightsSheetId, plgRegionHeaderRow + 1,   plgRegionHeaderRow + 1 + plgRegionDataCount,   1, 2, "$#,##0"),
    numFmtRange(insightsSheetId, plgIndustryHeaderRow + 1, plgIndustryHeaderRow + 1 + plgIndustryDataCount, 1, 2, "$#,##0"),
    numFmtRange(insightsSheetId, slTierHeaderRow + 1,      slTierHeaderRow + 1 + slTierDataCount,         1, 2, "$#,##0"),
    numFmtRange(insightsSheetId, slTierHeaderRow + 1,      slTierHeaderRow + 1 + slTierDataCount,         3, 4, "$#,##0"), // Avg ARR col
    numFmtRange(insightsSheetId, slRegionHeaderRow + 1,    slRegionHeaderRow + 1 + slRegionDataCount,     1, 2, "$#,##0"),
    numFmtRange(insightsSheetId, slIndustryHeaderRow + 1,  slIndustryHeaderRow + 1 + slIndustryDataCount, 1, 2, "$#,##0"),
    numFmtRange(insightsSheetId, growthTrendHeaderRow + 1, growthTrendHeaderRow + 1 + growthTrendDataCount, 1, 2, "$#,##0"),
    // AI cost has decimals: $#,##0.00
    numFmtRange(insightsSheetId, slAICostHeaderRow + 1,    slAICostHeaderRow + 1 + slAICostDataCount,     1, 2, "$#,##0.00"),
    // Integer / count columns (col B or C = index 1–2): #,##0
    numFmtRange(insightsSheetId, plgSeatHeaderRow + 1,  plgSeatHeaderRow + 5,   1, 2, "#,##0"),
    numFmtRange(insightsSheetId, slTierHeaderRow + 1,   slTierHeaderRow + 1 + slTierDataCount,     2, 3, "#,##0"), // Accounts
    numFmtRange(insightsSheetId, slRegionHeaderRow + 1, slRegionHeaderRow + 1 + slRegionDataCount, 2, 3, "#,##0"), // Accounts
    numFmtRange(insightsSheetId, slIndustryHeaderRow + 1, slIndustryHeaderRow + 1 + slIndustryDataCount, 2, 3, "#,##0"), // Accounts
    numFmtRange(insightsSheetId, slSeatHeaderRow + 1,   slSeatHeaderRow + 5,    1, 2, "#,##0"),
    numFmtRange(insightsSheetId, growthTrendHeaderRow + 1, growthTrendHeaderRow + 1 + growthTrendDataCount, 2, 3, "#,##0"), // Accounts
    numFmtRange(insightsSheetId, seatCompHeaderRow + 1, seatCompHeaderRow + 5,  1, 3, "#,##0"), // PLG + SL seat cols
    // KPI block — all value cells are now formula-based numbers in cols F–G (index 5–6)
    numFmtRange(insightsSheetId, kpiStartRow + 1, kpiStartRow + 2, 5, 7, "$#,##0"),    // Total ARR
    numFmtRange(insightsSheetId, kpiStartRow + 2, kpiStartRow + 3, 5, 7, "#,##0"),     // Accounts
    numFmtRange(insightsSheetId, kpiStartRow + 3, kpiStartRow + 4, 5, 7, "$#,##0"),    // Avg ARR
    numFmtRange(insightsSheetId, kpiStartRow + 4, kpiStartRow + 5, 5, 7, "#,##0"),     // Total Buzz MAU
    numFmtRange(insightsSheetId, kpiStartRow + 5, kpiStartRow + 6, 5, 7, "$#,##0.00"), // Avg AI Cost
  ];

  await batchUpdateSheet(formatRequests);

  // ── Add Charts ────────────────────────────────────────────────────────
  console.log("Adding charts...");

  // Charts are placed sequentially in column I (index 8), each anchored at
  // `chartRow` which advances after every chart so nothing overlaps.
  // Google Sheets default row height ≈ 21 px; we add 2 rows padding between charts.
  const CHART_COL = 8;
  const DEFAULT_ROW_PX = 21;
  const CHART_GAP_ROWS = 2;

  let chartRow = 1; // start just below the title row
  function nextChartAnchor(heightPx: number): number {
    const anchor = chartRow;
    chartRow += Math.ceil(heightPx / DEFAULT_ROW_PX) + CHART_GAP_ROWS;
    return anchor;
  }

  const chartRequests: AddChartRequest[] = [
    // 1. PLG ARR by Region (column chart)
    colChart("PLG: ARR by Region", {
      sheetId: insightsSheetId, headerRow: plgRegionHeaderRow,
      dataRows: plgRegionDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 280,
    }),

    // 2. PLG ARR by Industry (horizontal bar — many categories)
    barChart("PLG: ARR by Industry Group", {
      sheetId: insightsSheetId, headerRow: plgIndustryHeaderRow,
      dataRows: plgIndustryDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(440), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 440,
    }),

    // 3. PLG Seat Distribution (pie chart)
    pieChart("PLG: Seat Type Distribution", {
      sheetId: insightsSheetId, headerRow: plgSeatHeaderRow,
      dataRows: 4, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 440, heightPx: 280,
    }),

    // 4. Sales-Led ARR by Tier (column chart)
    colChart("Sales-Led: ARR by Tier", {
      sheetId: insightsSheetId, headerRow: slTierHeaderRow,
      dataRows: slTierDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 280,
    }),

    // 5. Sales-Led ARR by Region (column chart)
    colChart("Sales-Led: ARR by Region", {
      sheetId: insightsSheetId, headerRow: slRegionHeaderRow,
      dataRows: slRegionDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 280,
    }),

    // 6. Sales-Led Top 10 Industries (horizontal bar chart)
    barChart("Sales-Led: Top 10 Industries by ARR", {
      sheetId: insightsSheetId, headerRow: slIndustryHeaderRow,
      dataRows: slIndustryDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(400), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 400,
    }),

    // 7. Sales-Led Seat Distribution (pie chart)
    pieChart("Sales-Led: Seat Type Distribution", {
      sheetId: insightsSheetId, headerRow: slSeatHeaderRow,
      dataRows: 4, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 440, heightPx: 280,
    }),

    // 8. Sales-Led Avg AI Cost by Region (column chart)
    colChart("Sales-Led: Avg AI Cost/MAU by Region", {
      sheetId: insightsSheetId, headerRow: slAICostHeaderRow,
      dataRows: slAICostDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(280), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 280,
    }),

    // 9. ARR Trend by Industry Growth Rate — LINE chart across growth buckets
    lineChart("Sales-Led: ARR Trend by Industry Growth Rate", {
      sheetId: insightsSheetId, headerRow: growthTrendHeaderRow,
      dataRows: growthTrendDataCount, catCol: 0, valCol: 1,
      anchorRow: nextChartAnchor(300), anchorCol: CHART_COL,
      chartSheetId: insightsSheetId, widthPx: 500, heightPx: 300,
    }),

    // 10. PLG vs Sales-Led Seat Mix — multi-series column for comparison
    multiSeriesColChart(
      "PLG vs Sales-Led: Seat Mix Comparison",
      insightsSheetId, seatCompHeaderRow, seatCompDataCount, 0,
      [1, 2],
      [
        { red: 0.22, green: 0.44, blue: 0.78 },
        { red: 0.90, green: 0.45, blue: 0.13 },
      ],
      nextChartAnchor(300), CHART_COL, insightsSheetId, 500, 300
    ),
  ];

  await batchUpdateSheet(chartRequests);

  console.log("\n✅ Insights tab done!");

  // ── Build Marketing tab ────────────────────────────────────────────────
  console.log("  ⏸ Pausing 4s before Marketing tab...");
  await sleep(4000);
  await buildMarketingTab(plgRows, slRows);

  // ── Build Forecast tab ────────────────────────────────────────────────
  console.log("  ⏸ Pausing 4s before Forecast tab...");
  await sleep(4000);
  await buildForecastTab(plgRows, slRows);

  console.log("\n✅ All done! Open your Google Sheet:");
  console.log(`   https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/edit`);
}

main().catch((err: unknown) => {
  console.error("Fatal:", err);
  process.exit(1);
});
