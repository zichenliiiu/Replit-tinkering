# Replit tinkering

Google Sheets analysis script that builds an **Insights** tab and a **Marketing** tab with live formula-driven tables and embedded charts.

## Script
`scripts/src/sheets-charts.ts`

## How to run
```bash
SPREADSHEET_ID=<your-sheet-id> pnpm --filter @workspace/scripts run sheets-charts
```

## What it builds
- **Insights tab**: 10 summary tables + 10 charts (ARR, seats, MAU, AI cost by tier/region/industry)
- **Marketing tab**: 9 tables + 8 charts (marketing user penetration by seat type/region/industry, Sales-Led adoption rates)

All numeric cells use `SUMIF`/`COUNTIF`/`AVERAGEIF`/`IFERROR` formulas — nothing is hardcoded.