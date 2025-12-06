# Shopee Data Pipeline

A multi-source ETL pipeline for e-commerce order data with interactive dashboards.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full ETL pipeline
python main.py

# Export data for dashboard
python export_dashboard_data.py

# Run dashboard (React)
cd dashboard && npm install && npm run dev
```

## 📁 Project Structure

```
├── main.py                 # CLI entry point (multi-layer ETL)
├── app_gui.py              # Windows GUI app (standalone)
├── export_dashboard_data.py # Export data to JSON for dashboard
├── config.yaml             # Pipeline + sources config
├── src/
│   ├── etl/                # Multi-layer ETL architecture
│   │   ├── base.py         # Base ETL class
│   │   ├── pipeline.py     # Pipeline orchestrator
│   │   ├── bronze/         # Raw data extraction
│   │   ├── silver/         # Data cleaning & normalization
│   │   └── gold/           # Aggregation & metrics
│   ├── extract.py          # Legacy: Read Excel files
│   ├── transform.py        # Legacy: Data transformations
│   └── load.py             # Legacy: Save to Parquet
├── dashboard/              # React + Vite dashboard
│   ├── src/App.tsx         # Main dashboard application
│   └── public/data.json    # Exported data for dashboard
└── data/
    ├── raw/                # Input: Excel files by source
    ├── bronze/             # Layer 1: Raw extracted data
    ├── silver/             # Layer 2: Cleaned & normalized
    └── gold/               # Layer 3: Combined & aggregated
```

---

## 🏗️ Multi-Layer ETL Architecture (Medallion Architecture)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
└─────────────────────────────────────────────────────────────────────────────┘

   📁 RAW DATA                    🥉 BRONZE                    🥈 SILVER                    🥇 GOLD
   ──────────                    ────────                    ────────                    ────────
                                                                                    
   ┌─────────┐                 ┌─────────┐                 ┌─────────┐              ┌─────────────┐
   │ Shopee  │                 │ Bronze  │                 │ Silver  │              │    Gold     │
   │ Excel   │ ───Extract───▶  │ Shopee  │ ───Transform──▶ │ Shopee  │ ──Combine──▶ │ all_sources │
   │ files   │                 │ (raw)   │                 │(clean)  │              │  (unified)  │
   └─────────┘                 └─────────┘                 └─────────┘              └─────────────┘
                                                                                           │
   ┌─────────┐                 ┌─────────┐                 ┌─────────┐                     │
   │ Website │                 │ Bronze  │                 │ Silver  │                     │
   │ Excel   │ ───Extract───▶  │ Website │ ───Transform──▶ │ Website │ ────────────────────┤
   │ files   │                 │ (raw)   │                 │(clean)  │                     │
   └─────────┘                 └─────────┘                 └─────────┘                     │
                                                                                           │
   ┌─────────┐                 ┌─────────┐                 ┌─────────┐                     │
   │ Lazada  │                 │ Bronze  │                 │ Silver  │                     ▼
   │ Excel   │ ───Extract───▶  │ Lazada  │ ───Transform──▶ │ Lazada  │ ──────────▶  ┌─────────────┐
   │ files   │                 │ (raw)   │                 │(clean)  │              │   Metrics   │
   └─────────┘                 └─────────┘                 └─────────┘              │ Aggregations│
                                                                                    └─────────────┘
```

---

## 🥉 Bronze Layer (Raw Data)

**Purpose:** Extract raw data with minimal processing

| What it does | Why |
|--------------|-----|
| Read Excel files | Get data from source |
| Add `_source_file` column | Track data lineage |
| Convert Date to Datetime | Ensure consistent type |
| Add Year/Month columns | Enable partitioning |
| Add Source column | Identify data origin |

**Input:** `data/raw/{source}/*.xlsx`  
**Output:** `data/bronze/{source}/{year}/{month}/orders.parquet`

```python
# Bronze is "as-is" data - just extracted and typed
# No business logic, no cleaning
```

---

## 🥈 Silver Layer (Cleaned Data)

**Purpose:** Clean, normalize, and validate data

| What it does | Why |
|--------------|-----|
| Normalize Status | `"Cancel by cust."` → `"Cancelled"` |
| Clean strings | Remove whitespace |
| Validate data | Remove null dates, invalid rows |
| Select standard columns | Consistent schema |
| Source-specific rules | Handle each source's quirks |

**Input:** `data/bronze/{source}/**/*.parquet`  
**Output:** `data/silver/{source}/{year}/{month}/orders.parquet`

```python
# Silver is "business-ready" data
# Cleaned, validated, normalized
# Each source has its own Silver ETL for custom logic
```

### Status Normalization (config.yaml)

```yaml
status_mapping:
  "Delivered": "Delivered"
  "Cancel by cust.": "Cancelled"        # Shopee format
  "Cancelled by customer": "Cancelled"  # Website format
  "Failed delivery": "Failed"
  "Giao thành công": "Delivered"        # Vietnamese
```

---

## 🥇 Gold Layer (Aggregated Data)

**Purpose:** Combine all sources and create analytics-ready data

| What it does | Why |
|--------------|-----|
| Combine all Silver data | Unified view |
| Create metrics | Pre-calculated KPIs |
| Aggregate by time | Monthly summaries |
| Aggregate by source | Source comparison |

**Input:** `data/silver/*/**/*.parquet`  
**Output:** 
- `data/gold/all_sources/{year}/{month}/orders.parquet`
- `data/gold/metrics/monthly_by_source.parquet`
- `data/gold/metrics/cancellation_reasons.parquet`

```python
# Gold is "analytics-ready" data
# Pre-aggregated, optimized for dashboards
```

---

## 📊 Data Quality at Each Layer

| Layer | Quality | Use Case |
|-------|---------|----------|
| **Bronze** | Raw, may have issues | Debugging, reprocessing |
| **Silver** | Clean, validated | Source-specific analysis |
| **Gold** | Aggregated, unified | Dashboards, reports |

---

## 🔄 Pipeline Commands

```bash
# Full pipeline (all sources, all layers)
python main.py

# Process specific source only (Bronze + Silver)
python main.py --source shopee
python main.py --source website_suppersport

# Process specific layer only
python main.py --layer bronze    # Extract raw data
python main.py --layer silver    # Clean & normalize
python main.py --layer gold      # Combine & aggregate

# Combine: specific source + layer
python main.py --source shopee --layer silver
```

---

## ➕ Adding a New Data Source

### Option 1: Folder-Based (Simple)

Just create a folder with the source name:

```
data/raw/
├── shopee/           ← Folder name = Source name
│   ├── Jan 2025.xlsx
│   └── Feb 2025.xlsx
├── website/
│   └── Orders.xlsx
└── lazada/           ← New source!
    └── Sales.xlsx
```

Then run `python main.py` — auto-detects all sources!

### Option 2: Config-Based (Custom)

For custom patterns or column mappings, add to `config.yaml`:

```yaml
sources:
  lazada:
    pattern: "Lazada*.xlsx"
    source_name: "Lazada"
```

### Option 3: Custom ETL Class (Advanced)

For source-specific transformation logic:

```python
# src/etl/bronze/lazada.py
from src.etl.bronze.base import BronzeETL

class LazadaBronzeETL(BronzeETL):
    def transform(self, df):
        # Custom Lazada-specific logic
        return df
```

---

## 📈 Dashboard

React + Vite + Tailwind CSS + Recharts dashboard with three views:

1. **Overview** - Monthly trends, status distribution, cancellation reasons
2. **Compare** - Month-over-Month and Year-over-Year comparisons
3. **Sources** - Multi-source comparison

```bash
# Export data for dashboard
python export_dashboard_data.py

# Run dashboard
cd dashboard
npm install
npm run dev
```

---

## 🏗️ Windows Executable

Build standalone `.exe` for Windows:

```bash
# Install PyInstaller
pip install pyinstaller

# Build
python build_exe.py
# or
pyinstaller ShopeeDataPipeline.spec
```

See [BUILD_WINDOWS.md](BUILD_WINDOWS.md) for detailed instructions.

---

## 🔧 Development

### Linting & Formatting

```bash
# Python
ruff check .
ruff format .

# Pre-push hook (auto-runs on git push)
./setup-hooks.sh  # One-time setup
```

### Tech Stack

- **Python 3.11+** - Core language
- **Polars** - DataFrame library (NOT pandas)
- **FastExcel** - Excel reading
- **React 18 + TypeScript** - Dashboard UI
- **Vite** - Build tool
- **Tailwind CSS** - Styling
- **Recharts** - Charts
- **PyInstaller** - Windows exe builds

---

## 📋 Data Schema

Standard schema for all processed data:

| Column | Type | Description |
|--------|------|-------------|
| Date | Datetime | Order date |
| Source | String | Data source (Shopee, Website, etc.) |
| Order ID | String | Unique order identifier |
| Status | String | Original status |
| Status_Normalized | String | Standardized status |
| Reason cancelled | String | Cancellation reason |
| Year | Int | Extracted from Date |
| Month | Int | Extracted from Date |

---

## 🎯 Benefits of Multi-Layer Architecture

| Benefit | Description |
|---------|-------------|
| **Debuggable** | See data at each stage |
| **Reprocessable** | Fix errors at any layer |
| **Testable** | Test each source independently |
| **Extensible** | Add new sources easily |
| **Parallelizable** | Run Bronze/Silver in parallel |
| **Auditable** | Data lineage tracking |

---

## 📄 License

MIT

