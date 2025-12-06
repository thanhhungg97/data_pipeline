"""
Standalone GUI app for Shopee Data Pipeline.
Uses only tkinter (built into Python) - no extra GUI dependencies.
"""

import json
import os
import sys
import threading
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox


# Handle PyInstaller bundled paths
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and PyInstaller."""
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# ============================================================
# EMBEDDED ETL LOGIC (no external imports needed at runtime)
# ============================================================


def run_etl(input_dir: str, output_dir: str, progress_callback=None):
    """Run the full ETL pipeline."""
    import polars as pl
    import yaml

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    # Load status mapping from config (or use defaults)
    status_mapping = {
        "Delivered": "Delivered",
        "Completed": "Delivered",
        "Done": "Delivered",
        "Cancel by cust.": "Cancelled",
        "Cancelled": "Cancelled",
        "Canceled": "Cancelled",
        "Returned": "Returned",
        "Return": "Returned",
        "Refunded": "Returned",
        "Failed delivery": "Failed",
        "Failed": "Failed",
        "Delivery Failed": "Failed",
    }
    try:
        config_path = resource_path("config.yaml")
        if os.path.exists(config_path):
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
                status_mapping = config.get("status_mapping", status_mapping)
    except Exception:
        pass  # Use default mapping

    # EXTRACT
    log("📂 Reading Excel files...")
    input_path = Path(input_dir)
    dataframes = []

    excel_files = list(input_path.glob("*.xlsx")) + list(input_path.glob("*.xls"))

    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {input_dir}")

    for _i, file in enumerate(excel_files):
        log(f"  Reading {file.name}...")
        df = pl.read_excel(file)
        dataframes.append(df)

    log(f"✅ Loaded {len(excel_files)} files")

    # TRANSFORM
    log("🔄 Transforming data...")
    combined = pl.concat(dataframes, how="diagonal")

    # Add Year and Month
    combined = combined.with_columns(
        [
            pl.col("Date").dt.year().alias("Year"),
            pl.col("Date").dt.month().alias("Month"),
        ]
    )

    # Normalize Status
    combined = combined.with_columns(
        pl.col("Status")
        .map_elements(lambda x: status_mapping.get(x, x) if x else None, return_dtype=pl.Utf8)
        .alias("Status_Normalized")
    )

    log(f"✅ Combined {len(combined):,} rows")

    # LOAD - Partitioned by Year/Month
    log("💾 Saving partitioned files...")
    output_path = Path(output_dir)

    # Filter out null dates
    df_valid = combined.filter(pl.col("Year").is_not_null() & pl.col("Month").is_not_null())
    unique_periods = df_valid.select(["Year", "Month"]).unique().sort(["Year", "Month"])

    total_files = 0
    for row in unique_periods.iter_rows(named=True):
        year = int(row["Year"])
        month = int(row["Month"])

        partition = df_valid.filter((pl.col("Year") == year) & (pl.col("Month") == month))

        partition_path = output_path / str(year) / f"{month:02d}" / "orders.parquet"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        partition.write_parquet(partition_path)
        total_files += 1
        log(f"  Saved {year}/{month:02d} ({len(partition):,} rows)")

    log(f"\n✅ Pipeline complete! {len(df_valid):,} rows → {total_files} files")
    return output_path


def generate_dashboard(data_dir: str, output_file: str = "dashboard.html"):
    """Generate the comparison dashboard HTML."""
    import polars as pl

    path = Path(data_dir)
    all_files = list(path.glob("**/*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    df = pl.concat([pl.read_parquet(f) for f in all_files])

    # Use Status_Normalized if available (from new pipeline), fallback to Status
    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    # Calculate monthly metrics using normalized status
    monthly = (
        df.group_by(["Year", "Month"])
        .agg(
            [
                pl.len().alias("total_orders"),
                pl.col(status_col)
                .filter(pl.col(status_col) == "Delivered")
                .len()
                .alias("delivered"),
                pl.col(status_col)
                .filter(pl.col(status_col) == "Cancelled")
                .len()
                .alias("cancelled"),
                pl.col(status_col).filter(pl.col(status_col) == "Returned").len().alias("returned"),
                pl.col(status_col).filter(pl.col(status_col) == "Failed").len().alias("failed"),
            ]
        )
        .sort(["Year", "Month"])
    )

    monthly = monthly.with_columns(
        [
            (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
            (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
        ]
    )

    data_js = monthly.to_dicts()

    # Dashboard HTML template
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shopee Orders Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', system-ui, sans-serif; background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); min-height: 100vh; color: #e0e0e0; }
        .header { background: rgba(255,255,255,0.05); padding: 20px 40px; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .header h1 { font-size: 28px; font-weight: 600; color: #fff; }
        .controls { padding: 20px 40px; }
        select { padding: 10px 20px; font-size: 16px; border: 1px solid rgba(255,255,255,0.2); border-radius: 8px; background: rgba(255,255,255,0.1); color: #fff; cursor: pointer; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; padding: 20px 40px; }
        .metric-card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 24px; border: 1px solid rgba(255,255,255,0.1); }
        .metric-card h3 { font-size: 14px; color: #888; text-transform: uppercase; margin-bottom: 10px; }
        .metric-value { font-size: 36px; font-weight: 700; color: #fff; }
        .comparisons { display: flex; gap: 20px; margin-top: 15px; }
        .comparison { display: flex; flex-direction: column; }
        .comparison-label { font-size: 11px; color: #666; }
        .comparison-value { font-size: 14px; font-weight: 600; }
        .positive { color: #00d4aa; }
        .negative { color: #ff6b6b; }
        .neutral { color: #888; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; padding: 20px 40px; }
        .status-item { background: rgba(255,255,255,0.05); border-radius: 12px; padding: 16px; border: 1px solid rgba(255,255,255,0.1); }
        .status-item h4 { font-size: 13px; color: #888; margin-bottom: 8px; }
        .status-item .pct { font-size: 28px; font-weight: 700; }
        .status-item .value { font-size: 14px; color: #888; margin-left: 8px; }
        .delivered { color: #00d4aa; }
        .cancelled { color: #ff6b6b; }
        .returned { color: #ffa726; }
        .failed { color: #ab47bc; }
        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; padding: 20px 40px; }
        .chart-card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 20px; border: 1px solid rgba(255,255,255,0.1); }
    </style>
</head>
<body>
    <div class="header"><h1>🛒 Shopee Orders Dashboard</h1></div>
    <div class="controls"><select id="monthSelector" onchange="updateDashboard()"></select></div>
    <div class="metrics-grid">
        <div class="metric-card"><h3>Total Orders</h3><div class="metric-value" id="totalOrders">-</div><div class="comparisons"><div class="comparison"><span class="comparison-label">vs Last Month</span><span class="comparison-value" id="totalVsLastMonth">-</span></div><div class="comparison"><span class="comparison-label">vs Last Year</span><span class="comparison-value" id="totalVsLastYear">-</span></div></div></div>
        <div class="metric-card"><h3>Delivery Rate</h3><div class="metric-value" id="deliveryRate">-</div><div class="comparisons"><div class="comparison"><span class="comparison-label">vs Last Month</span><span class="comparison-value" id="deliveryVsLastMonth">-</span></div><div class="comparison"><span class="comparison-label">vs Last Year</span><span class="comparison-value" id="deliveryVsLastYear">-</span></div></div></div>
        <div class="metric-card"><h3>Cancel Rate</h3><div class="metric-value" id="cancelRate">-</div><div class="comparisons"><div class="comparison"><span class="comparison-label">vs Last Month</span><span class="comparison-value" id="cancelVsLastMonth">-</span></div><div class="comparison"><span class="comparison-label">vs Last Year</span><span class="comparison-value" id="cancelVsLastYear">-</span></div></div></div>
    </div>
    <div class="status-grid">
        <div class="status-item"><h4>✅ Delivered</h4><span class="pct delivered" id="deliveredPct">-</span><span class="value" id="deliveredCount">-</span></div>
        <div class="status-item"><h4>❌ Cancelled</h4><span class="pct cancelled" id="cancelledPct">-</span><span class="value" id="cancelledCount">-</span></div>
        <div class="status-item"><h4>↩️ Returned</h4><span class="pct returned" id="returnedPct">-</span><span class="value" id="returnedCount">-</span></div>
        <div class="status-item"><h4>⚠️ Failed</h4><span class="pct failed" id="failedPct">-</span><span class="value" id="failedCount">-</span></div>
    </div>
    <div class="charts-grid">
        <div class="chart-card"><div id="trendChart"></div></div>
        <div class="chart-card"><div id="comparisonChart"></div></div>
    </div>
    <script>
        const monthlyData = DATA_PLACEHOLDER;
        monthlyData.sort((a, b) => a.Year !== b.Year ? a.Year - b.Year : a.Month - b.Month);
        const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const selector = document.getElementById('monthSelector');
        [...monthlyData].reverse().forEach((d, i) => {
            const opt = document.createElement('option');
            opt.value = `${d.Year}-${d.Month}`;
            opt.textContent = `${monthNames[d.Month]} ${d.Year}`;
            if (i === 0) opt.selected = true;
            selector.appendChild(opt);
        });
        function getMonthData(y, m) { return monthlyData.find(d => d.Year === y && d.Month === m); }
        function getLastMonth(y, m) { return m === 1 ? {year: y-1, month: 12} : {year: y, month: m-1}; }
        function fmt(n) { return n.toLocaleString(); }
        function fmtChange(cur, prev, isRate=false) {
            if (!prev) return '<span class="neutral">N/A</span>';
            const chg = isRate ? (cur - prev).toFixed(1) : (((cur - prev) / prev) * 100).toFixed(1);
            const cls = chg > 0 ? (isRate ? 'negative' : 'positive') : chg < 0 ? (isRate ? 'positive' : 'negative') : 'neutral';
            return `<span class="${cls}">${chg > 0 ? '↑' : '↓'} ${Math.abs(chg)}${isRate ? 'pp' : '%'}</span>`;
        }
        function updateDashboard() {
            const [y, m] = selector.value.split('-').map(Number);
            const cur = getMonthData(y, m);
            if (!cur) return;
            const lmInfo = getLastMonth(y, m);
            const lm = getMonthData(lmInfo.year, lmInfo.month);
            const ly = getMonthData(y - 1, m);
            document.getElementById('totalOrders').textContent = fmt(cur.total_orders);
            document.getElementById('deliveryRate').textContent = cur.delivery_rate + '%';
            document.getElementById('cancelRate').textContent = cur.cancel_rate + '%';
            document.getElementById('totalVsLastMonth').innerHTML = fmtChange(cur.total_orders, lm?.total_orders);
            document.getElementById('totalVsLastYear').innerHTML = fmtChange(cur.total_orders, ly?.total_orders);
            document.getElementById('deliveryVsLastMonth').innerHTML = fmtChange(cur.delivery_rate, lm?.delivery_rate, true);
            document.getElementById('deliveryVsLastYear').innerHTML = fmtChange(cur.delivery_rate, ly?.delivery_rate, true);
            const cVsLM = lm ? (cur.cancel_rate - lm.cancel_rate).toFixed(1) : null;
            const cVsLY = ly ? (cur.cancel_rate - ly.cancel_rate).toFixed(1) : null;
            document.getElementById('cancelVsLastMonth').innerHTML = cVsLM !== null ? `<span class="${cVsLM > 0 ? 'negative' : 'positive'}">${cVsLM > 0 ? '↑' : '↓'} ${Math.abs(cVsLM)}pp</span>` : '<span class="neutral">N/A</span>';
            document.getElementById('cancelVsLastYear').innerHTML = cVsLY !== null ? `<span class="${cVsLY > 0 ? 'negative' : 'positive'}">${cVsLY > 0 ? '↑' : '↓'} ${Math.abs(cVsLY)}pp</span>` : '<span class="neutral">N/A</span>';
            document.getElementById('deliveredPct').textContent = `${cur.delivery_rate}%`;
            document.getElementById('deliveredCount').textContent = `(${fmt(cur.delivered)})`;
            document.getElementById('cancelledPct').textContent = `${cur.cancel_rate}%`;
            document.getElementById('cancelledCount').textContent = `(${fmt(cur.cancelled)})`;
            document.getElementById('returnedPct').textContent = `${(cur.returned/cur.total_orders*100).toFixed(1)}%`;
            document.getElementById('returnedCount').textContent = `(${fmt(cur.returned)})`;
            document.getElementById('failedPct').textContent = `${(cur.failed/cur.total_orders*100).toFixed(1)}%`;
            document.getElementById('failedCount').textContent = `(${fmt(cur.failed)})`;
            updateCharts(y, m, lm, ly);
        }
        function updateCharts(y, m, lm, ly) {
            const curIdx = monthlyData.findIndex(d => d.Year === y && d.Month === m);
            const trend = monthlyData.slice(Math.max(0, curIdx - 11), curIdx + 1);
            const labels = trend.map(d => `${monthNames[d.Month]} ${d.Year}`);
            Plotly.newPlot('trendChart', [{x: labels, y: trend.map(d => d.total_orders), type: 'scatter', mode: 'lines+markers', name: 'Orders', line: {color: '#00d4aa', width: 3}}], {paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: {color: '#888'}, margin: {t: 30, r: 20, b: 60, l: 60}, xaxis: {gridcolor: 'rgba(255,255,255,0.1)', tickangle: -45}, yaxis: {gridcolor: 'rgba(255,255,255,0.1)'}}, {responsive: true});
            const cur = getMonthData(y, m);
            const cats = ['Total', 'Delivered', 'Cancelled'];
            Plotly.newPlot('comparisonChart', [
                {x: cats, y: [cur.total_orders, cur.delivered, cur.cancelled], type: 'bar', name: `${monthNames[m]} ${y}`, marker: {color: '#00d4aa'}},
                {x: cats, y: lm ? [lm.total_orders, lm.delivered, lm.cancelled] : [0,0,0], type: 'bar', name: 'Last Month', marker: {color: '#4a90a4'}},
                {x: cats, y: ly ? [ly.total_orders, ly.delivered, ly.cancelled] : [0,0,0], type: 'bar', name: 'Last Year', marker: {color: '#8b5cf6'}}
            ], {paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: {color: '#888'}, margin: {t: 30, r: 20, b: 40, l: 60}, barmode: 'group', legend: {orientation: 'h', y: 1.15}}, {responsive: true});
        }
        updateDashboard();
    </script>
</body>
</html>"""

    html_content = html_template.replace("DATA_PLACEHOLDER", json.dumps(data_js))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    return output_file


# ============================================================
# GUI APPLICATION
# ============================================================


class DataPipelineApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Shopee Data Pipeline")
        self.root.geometry("600x500")
        self.root.configure(bg="#1a1a2e")

        # Variables
        self.input_dir = tk.StringVar()
        self.output_dir = tk.StringVar(value=str(Path.home() / "ShopeeData" / "processed"))

        self.setup_ui()

    def setup_ui(self):
        # Title
        title = tk.Label(
            self.root,
            text="🛒 Shopee Data Pipeline",
            font=("Segoe UI", 20, "bold"),
            bg="#1a1a2e",
            fg="white",
        )
        title.pack(pady=20)

        # Input folder
        frame1 = tk.Frame(self.root, bg="#1a1a2e")
        frame1.pack(fill="x", padx=40, pady=10)

        tk.Label(
            frame1,
            text="Input Folder (Excel files):",
            bg="#1a1a2e",
            fg="#888",
            font=("Segoe UI", 10),
        ).pack(anchor="w")

        input_frame = tk.Frame(frame1, bg="#1a1a2e")
        input_frame.pack(fill="x", pady=5)

        self.input_entry = tk.Entry(
            input_frame, textvariable=self.input_dir, font=("Segoe UI", 11), width=45
        )
        self.input_entry.pack(side="left", fill="x", expand=True)

        tk.Button(
            input_frame, text="Browse", command=self.browse_input, font=("Segoe UI", 10)
        ).pack(side="right", padx=(10, 0))

        # Output folder
        frame2 = tk.Frame(self.root, bg="#1a1a2e")
        frame2.pack(fill="x", padx=40, pady=10)

        tk.Label(
            frame2, text="Output Folder:", bg="#1a1a2e", fg="#888", font=("Segoe UI", 10)
        ).pack(anchor="w")

        output_frame = tk.Frame(frame2, bg="#1a1a2e")
        output_frame.pack(fill="x", pady=5)

        self.output_entry = tk.Entry(
            output_frame, textvariable=self.output_dir, font=("Segoe UI", 11), width=45
        )
        self.output_entry.pack(side="left", fill="x", expand=True)

        tk.Button(
            output_frame, text="Browse", command=self.browse_output, font=("Segoe UI", 10)
        ).pack(side="right", padx=(10, 0))

        # Run button
        self.run_btn = tk.Button(
            self.root,
            text="▶ Run Pipeline",
            command=self.run_pipeline,
            font=("Segoe UI", 12, "bold"),
            bg="#00d4aa",
            fg="white",
            padx=30,
            pady=10,
            cursor="hand2",
        )
        self.run_btn.pack(pady=20)

        # Progress log
        log_frame = tk.Frame(self.root, bg="#1a1a2e")
        log_frame.pack(fill="both", expand=True, padx=40, pady=10)

        tk.Label(log_frame, text="Progress:", bg="#1a1a2e", fg="#888", font=("Segoe UI", 10)).pack(
            anchor="w"
        )

        self.log_text = tk.Text(
            log_frame, height=12, bg="#16213e", fg="#e0e0e0", font=("Consolas", 10), wrap="word"
        )
        self.log_text.pack(fill="both", expand=True, pady=5)

        # Dashboard button (initially hidden)
        self.dashboard_btn = tk.Button(
            self.root,
            text="📊 Open Dashboard",
            command=self.open_dashboard,
            font=("Segoe UI", 11),
            bg="#4a90a4",
            fg="white",
            padx=20,
            pady=8,
        )
        self.dashboard_path = None

    def browse_input(self):
        folder = filedialog.askdirectory(title="Select folder with Excel files")
        if folder:
            self.input_dir.set(folder)

    def browse_output(self):
        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.output_dir.set(folder)

    def log(self, message):
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.root.update_idletasks()

    def run_pipeline(self):
        if not self.input_dir.get():
            messagebox.showerror("Error", "Please select an input folder")
            return

        self.run_btn.config(state="disabled", text="Running...")
        self.log_text.delete("1.0", "end")

        def task():
            try:
                # Run ETL
                output_path = run_etl(
                    self.input_dir.get(), self.output_dir.get(), progress_callback=self.log
                )

                # Generate dashboard
                self.log("\n📊 Generating dashboard...")
                dashboard_path = Path(self.output_dir.get()) / "dashboard.html"
                generate_dashboard(str(output_path), str(dashboard_path))
                self.dashboard_path = str(dashboard_path)

                self.log(f"✅ Dashboard saved to: {dashboard_path}")

                # Show dashboard button
                self.root.after(0, lambda: self.dashboard_btn.pack(pady=10))

            except Exception as e:
                self.log(f"\n❌ Error: {str(e)}")
                messagebox.showerror("Error", str(e))
            finally:
                self.root.after(
                    0, lambda: self.run_btn.config(state="normal", text="▶ Run Pipeline")
                )

        threading.Thread(target=task, daemon=True).start()

    def open_dashboard(self):
        if self.dashboard_path:
            webbrowser.open(f"file://{self.dashboard_path}")


def main():
    root = tk.Tk()
    DataPipelineApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
