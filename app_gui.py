"""
Standalone GUI app for Data Processing Pipeline.
Uses only tkinter (built into Python) - no extra GUI dependencies.
Shares ETL logic with CLI via src/etl/pipeline.py
"""

import json
import os
import shutil
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
# IMPORT SHARED ETL LOGIC
# ============================================================

# Add project root to path for imports
sys.path.insert(0, resource_path("."))

from src.etl.pipeline import generate_metrics, run_simple_etl  # noqa: E402

# ============================================================
# DASHBOARD DEPLOYMENT
# ============================================================


def export_data_json(metrics: list[dict], output_dir: str) -> str:
    """Export metrics as data.json for React dashboard."""
    # Build data structure expected by React dashboard
    data = {
        "monthly": metrics,
        "reasons": [],  # Simple ETL doesn't extract reasons
        "metrics": [
            {
                "Source": "All",
                "Year": m["Year"],
                "Month": m["Month"],
                "total_orders": m["total_orders"],
                "delivered": m["delivered"],
                "cancelled": m["cancelled"],
                "returned": m["returned"],
                "failed": m["failed"],
                "delivery_rate": m["delivery_rate"],
                "cancel_rate": m["cancel_rate"],
            }
            for m in metrics
        ],
        "sources": [],
    }

    output_path = Path(output_dir) / "dashboard" / "data.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    return str(output_path)


def deploy_react_dashboard(output_dir: str, progress_callback=None) -> str:
    """Copy bundled React dashboard to output directory."""

    def log(msg):
        if progress_callback:
            progress_callback(msg)

    # Source: bundled dashboard/dist or local dashboard/dist
    bundled_dist = resource_path("dashboard_dist")
    local_dist = resource_path("dashboard/dist")

    if os.path.exists(bundled_dist):
        src_dir = bundled_dist
    elif os.path.exists(local_dist):
        src_dir = local_dist
    else:
        log("⚠️ React dashboard not found, falling back to simple HTML")
        return None

    # Destination
    dest_dir = Path(output_dir) / "dashboard"

    # Copy dashboard files
    if dest_dir.exists():
        shutil.rmtree(dest_dir)

    shutil.copytree(src_dir, dest_dir)
    log(f"  ✓ Copied React dashboard to {dest_dir}")

    return str(dest_dir / "index.html")


# ============================================================
# FALLBACK: Simple HTML Dashboard
# ============================================================


def generate_simple_dashboard(metrics: list[dict], output_file: str = "dashboard.html"):
    """Generate a simple HTML dashboard as fallback."""

    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Orders Dashboard</title>
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
        .positive { color: #00d4aa; }
        .negative { color: #ff6b6b; }
        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; padding: 20px 40px; }
        .chart-card { background: rgba(255,255,255,0.05); border-radius: 16px; padding: 20px; border: 1px solid rgba(255,255,255,0.1); }
    </style>
</head>
<body>
    <div class="header"><h1>📊 Orders Dashboard</h1></div>
    <div class="controls"><select id="monthSelector" onchange="updateDashboard()"></select></div>
    <div class="metrics-grid">
        <div class="metric-card"><h3>Total Orders</h3><div class="metric-value" id="totalOrders">-</div></div>
        <div class="metric-card"><h3>Delivery Rate</h3><div class="metric-value" id="deliveryRate">-</div></div>
        <div class="metric-card"><h3>Cancel Rate</h3><div class="metric-value" id="cancelRate">-</div></div>
    </div>
    <div class="charts-grid">
        <div class="chart-card"><div id="trendChart"></div></div>
        <div class="chart-card"><div id="statusChart"></div></div>
    </div>
    <script>
        const data = DATA_PLACEHOLDER;
        data.sort((a, b) => a.Year !== b.Year ? a.Year - b.Year : a.Month - b.Month);
        const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const selector = document.getElementById('monthSelector');
        [...data].reverse().forEach((d, i) => {
            const opt = document.createElement('option');
            opt.value = `${d.Year}-${d.Month}`;
            opt.textContent = `${monthNames[d.Month]} ${d.Year}`;
            if (i === 0) opt.selected = true;
            selector.appendChild(opt);
        });
        function updateDashboard() {
            const [y, m] = selector.value.split('-').map(Number);
            const cur = data.find(d => d.Year === y && d.Month === m);
            if (!cur) return;
            document.getElementById('totalOrders').textContent = cur.total_orders.toLocaleString();
            document.getElementById('deliveryRate').textContent = cur.delivery_rate + '%';
            document.getElementById('cancelRate').textContent = cur.cancel_rate + '%';
            // Trend chart
            const idx = data.findIndex(d => d.Year === y && d.Month === m);
            const trend = data.slice(Math.max(0, idx - 11), idx + 1);
            Plotly.newPlot('trendChart', [{x: trend.map(d => `${monthNames[d.Month]} ${d.Year}`), y: trend.map(d => d.total_orders), type: 'scatter', mode: 'lines+markers', line: {color: '#00d4aa'}}], {paper_bgcolor: 'transparent', plot_bgcolor: 'transparent', font: {color: '#888'}, margin: {t: 30, b: 60, l: 60, r: 20}, xaxis: {tickangle: -45}});
            // Status chart
            Plotly.newPlot('statusChart', [{values: [cur.delivered, cur.cancelled, cur.returned, cur.failed], labels: ['Delivered', 'Cancelled', 'Returned', 'Failed'], type: 'pie', marker: {colors: ['#00d4aa', '#ff6b6b', '#ffa726', '#ab47bc']}}], {paper_bgcolor: 'transparent', font: {color: '#888'}, margin: {t: 30, b: 30}});
        }
        updateDashboard();
    </script>
</body>
</html>"""

    html_content = html_template.replace("DATA_PLACEHOLDER", json.dumps(metrics))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    return output_file


# ============================================================
# GUI APPLICATION
# ============================================================


class DataPipelineApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Processing Pipeline")
        self.root.geometry("600x500")
        self.root.configure(bg="#1a1a2e")

        # Variables
        self.input_dir = tk.StringVar()
        self.output_dir = tk.StringVar(value=str(Path.home() / "DataPipeline" / "processed"))

        self.setup_ui()

    def setup_ui(self):
        # Title
        title = tk.Label(
            self.root,
            text="📊 Data Processing Pipeline",
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
                # Get config path
                config_path = resource_path("config.yaml")

                # Run shared ETL pipeline
                run_simple_etl(
                    input_dir=self.input_dir.get(),
                    output_dir=self.output_dir.get(),
                    config_path=config_path,
                    progress_callback=self.log,
                )

                # Generate metrics
                self.log("\n📊 Generating dashboard...")
                metrics = generate_metrics(
                    data_dir=self.output_dir.get(),
                    progress_callback=self.log,
                )

                # Try to deploy React dashboard
                self.log("  Deploying React dashboard...")
                dashboard_html = deploy_react_dashboard(
                    output_dir=self.output_dir.get(),
                    progress_callback=self.log,
                )

                if dashboard_html:
                    # Export data.json for React dashboard
                    export_data_json(metrics, self.output_dir.get())
                    self.dashboard_path = dashboard_html
                    self.log(f"✅ React dashboard ready: {dashboard_html}")
                else:
                    # Fallback to simple HTML dashboard
                    self.log("  Using simple HTML dashboard...")
                    dashboard_path = Path(self.output_dir.get()) / "dashboard.html"
                    generate_simple_dashboard(metrics, str(dashboard_path))
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
