"""
Modern GUI app for Data Processing Pipeline.
Uses CustomTkinter for modern UI design.
Supports multiple sources with custom names and paths.
"""

import json
import os
import shutil
import sys
import threading
import webbrowser
from pathlib import Path

import customtkinter as ctk
import yaml

from dashboard_server import get_dashboard_server

# Set appearance
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


# Handle PyInstaller bundled paths
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and PyInstaller."""
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# ============================================================
# LAZY IMPORT FOR FASTER STARTUP
# ============================================================

sys.path.insert(0, resource_path("."))

# Don't import polars/ETL at startup - import when needed
_etl_module = None


def get_etl_function():
    """Lazy load ETL to speed up app startup."""
    global _etl_module
    if _etl_module is None:
        from src.etl import pipeline

        _etl_module = pipeline
    return _etl_module.run_simple_etl_files


# ============================================================
# MULTI-SOURCE ETL
# ============================================================


def run_multi_source_etl(
    sources: list[dict],
    output_dir: str,
    config_path: str = "config.yaml",
    progress_callback=None,
    source_callback=None,
):
    """Run ETL for multiple sources with error handling."""
    import traceback

    import polars as pl

    def log(msg: str):
        if progress_callback:
            progress_callback(msg)

    def update_source(name: str, status: str):
        if source_callback:
            source_callback(name, status)

    output_path = Path(output_dir)
    all_dataframes = []

    results = {
        "successful": [],
        "failed": [],
        "errors": {},
        "total_rows": 0,
    }

    if not os.path.exists(config_path):
        log(f"⚠️ Config not found at {config_path}, using defaults")

    log(f"🚀 Starting pipeline for {len(sources)} source(s)...")

    for i, source in enumerate(sources, 1):
        name = source["name"]
        path = source.get("path", "")
        files = source.get("files", [])

        update_source(name, "processing")
        log(f"\n{'─' * 40}")
        log(f"📂 [{i}/{len(sources)}] {name}")

        try:
            # Get files either from selection or from folder
            if files:
                excel_files = [Path(f) for f in files if os.path.exists(f)]
                log(f"   {len(excel_files)} selected files")
            elif path and os.path.isdir(path):
                excel_files = list(Path(path).glob("*.xlsx")) + list(Path(path).glob("*.xls"))
                log(f"   Folder: {path}")
                log(f"   Found {len(excel_files)} file(s)")
            else:
                raise FileNotFoundError("No files or folder selected")

            if not excel_files:
                raise FileNotFoundError("No valid Excel files found")

            source_output = output_path / name.lower().replace(" ", "_")
            run_etl = get_etl_function()
            etl_result = run_etl(
                files=excel_files,
                output_dir=str(source_output),
                config_path=config_path,
                progress_callback=log,
            )

            parquet_files = list(source_output.glob("**/*.parquet"))
            if parquet_files:
                df = pl.concat([pl.read_parquet(f) for f in parquet_files])
                df = df.with_columns(pl.lit(name).alias("Source"))
                all_dataframes.append(df)
                rows = len(df)
            else:
                rows = 0

            # Check for file-level errors/warnings
            file_errors = etl_result.get("file_errors", [])
            has_errors = any(not e.get("warning") for e in file_errors)

            if has_errors:
                update_source(name, "warning")
                log(f"⚠️ {name}: {rows:,} rows (with warnings)")
                results["successful"].append(name)
                results["warnings"] = results.get("warnings", {})
                results["warnings"][name] = file_errors
            else:
                update_source(name, "done")
                log(f"✅ {name}: {rows:,} rows")
                results["successful"].append(name)

            results["total_rows"] += rows

        except FileNotFoundError as e:
            update_source(name, "error")
            log(f"❌ {name}: {str(e)}")
            results["failed"].append(name)
            results["errors"][name] = {"type": "FileNotFound", "message": str(e)}

        except Exception as e:
            update_source(name, "error")
            error_trace = traceback.format_exc()
            log(f"❌ {name}: {str(e)}")
            results["failed"].append(name)
            results["errors"][name] = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": error_trace,
            }

    if all_dataframes:
        log(f"\n{'─' * 40}")
        log("🔄 Combining sources...")

        combined = pl.concat(all_dataframes, how="diagonal")
        combined_output = output_path / "all_sources"
        combined_output.mkdir(parents=True, exist_ok=True)

        if "Year" in combined.columns and "Month" in combined.columns:
            unique_periods = (
                combined.filter(pl.col("Year").is_not_null())
                .select(["Year", "Month"])
                .unique()
                .sort(["Year", "Month"])
            )

            for row in unique_periods.iter_rows(named=True):
                year, month = int(row["Year"]), int(row["Month"])
                partition = combined.filter((pl.col("Year") == year) & (pl.col("Month") == month))
                part_path = combined_output / str(year) / f"{month:02d}" / "orders.parquet"
                part_path.parent.mkdir(parents=True, exist_ok=True)
                partition.write_parquet(part_path)

        log(f"✅ Combined {len(combined):,} rows")

    log(f"\n{'═' * 40}")
    log("📊 SUMMARY")
    log(f"{'═' * 40}")

    warnings = results.get("warnings", {})
    success_clean = len([s for s in results["successful"] if s not in warnings])
    success_warnings = len([s for s in results["successful"] if s in warnings])

    if success_clean > 0:
        log(f"✅ Success: {success_clean} source(s)")
    if success_warnings > 0:
        log(f"⚠️ With warnings: {success_warnings} source(s)")
        for source_name, file_errors in warnings.items():
            log(f"   {source_name}:")
            for err in file_errors[:3]:  # Show first 3 errors
                log(f"     • {err['file']}: {err['error']}")
            if len(file_errors) > 3:
                log(f"     ... and {len(file_errors) - 3} more")

    log(f"❌ Failed: {len(results['failed'])}")
    log(f"📁 Total: {results['total_rows']:,} rows")

    results["output_dir"] = str(output_path)
    return results


# ============================================================
# DASHBOARD FUNCTIONS
# ============================================================


def export_data_json(data_dir: str) -> str:
    """Export metrics as data.json for React dashboard."""
    import polars as pl

    path = Path(data_dir)
    all_files = list(path.glob("**/*.parquet"))
    if not all_files:
        return None

    df = pl.concat([pl.read_parquet(f) for f in all_files])
    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    sources = []
    if "Source" in df.columns:
        sources = sorted(df["Source"].unique().to_list())

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

    metrics_data = []
    if "Source" in df.columns:
        by_source = (
            df.group_by(["Source", "Year", "Month"])
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
                    pl.col(status_col)
                    .filter(pl.col(status_col) == "Returned")
                    .len()
                    .alias("returned"),
                    pl.col(status_col).filter(pl.col(status_col) == "Failed").len().alias("failed"),
                ]
            )
            .sort(["Source", "Year", "Month"])
        )
        by_source = by_source.with_columns(
            [
                (pl.col("delivered") / pl.col("total_orders") * 100)
                .round(1)
                .alias("delivery_rate"),
                (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
            ]
        )
        metrics_data = by_source.to_dicts()

    data = {
        "monthly": monthly.to_dicts(),
        "metrics": metrics_data,
        "sources": sources,
        "reasons": [],
    }

    # Also write to file for reference
    output_path = Path(data_dir) / "dashboard" / "data.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    return data  # Return data dict for embedding


def deploy_react_dashboard(output_dir: str, data: dict) -> str:
    """Deploy React dashboard and return the directory path."""
    bundled_dist = resource_path("dashboard_dist")
    local_dist = resource_path("dashboard/dist")

    src_dir = None
    if os.path.exists(bundled_dist):
        src_dir = bundled_dist
    elif os.path.exists(local_dist):
        src_dir = local_dist

    if not src_dir:
        return None

    dest_dir = Path(output_dir) / "dashboard"
    if dest_dir.exists():
        shutil.rmtree(dest_dir)

    shutil.copytree(src_dir, dest_dir)

    # Write data.json for the dashboard to fetch via HTTP
    data_path = dest_dir / "data.json"
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    return str(dest_dir)


# ============================================================
# SOURCE CARD WIDGET
# ============================================================


class SourceCard(ctk.CTkFrame):
    """Modern card widget for a single source."""

    STATUS_COLORS = {
        "pending": "#6b7280",
        "processing": "#f59e0b",
        "done": "#10b981",
        "warning": "#eab308",
        "error": "#ef4444",
    }

    def __init__(self, parent, on_remove, card_id):
        super().__init__(parent, corner_radius=12, fg_color="#1e293b")
        self.on_remove = on_remove
        self.card_id = card_id
        self.status = "pending"
        self.selected_files = []  # Store selected files

        # Main container
        self.grid_columnconfigure(1, weight=1)

        # Status indicator
        self.status_frame = ctk.CTkFrame(self, width=8, corner_radius=4, fg_color="#6b7280")
        self.status_frame.grid(row=0, column=0, rowspan=3, sticky="ns", padx=(12, 8), pady=12)

        # Content
        content = ctk.CTkFrame(self, fg_color="transparent")
        content.grid(row=0, column=1, sticky="ew", padx=(0, 12), pady=12)
        content.grid_columnconfigure(0, weight=1)

        # Header row with name and remove button
        header = ctk.CTkFrame(content, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew")
        header.grid_columnconfigure(0, weight=1)

        self.name_var = ctk.StringVar(value=f"Source {card_id}")
        self.name_entry = ctk.CTkEntry(
            header,
            textvariable=self.name_var,
            placeholder_text="Source name",
            height=36,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        self.name_entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))

        remove_btn = ctk.CTkButton(
            header,
            text="✕",
            width=36,
            height=36,
            corner_radius=8,
            fg_color="#374151",
            hover_color="#ef4444",
            command=self._remove,
        )
        remove_btn.grid(row=0, column=1)

        # Path/Files display
        self.path_var = ctk.StringVar()
        self.path_entry = ctk.CTkEntry(
            content,
            textvariable=self.path_var,
            placeholder_text="Select folder or files...",
            height=36,
            state="readonly",
        )
        self.path_entry.grid(row=1, column=0, sticky="ew", pady=(8, 0))

        # Button row - Folder and Files buttons
        btn_frame = ctk.CTkFrame(content, fg_color="transparent")
        btn_frame.grid(row=2, column=0, sticky="ew", pady=(8, 0))

        ctk.CTkButton(
            btn_frame,
            text="📁 Folder",
            width=90,
            height=32,
            corner_radius=8,
            fg_color="#374151",
            hover_color="#4b5563",
            command=self._browse_folder,
        ).pack(side="left", padx=(0, 8))

        ctk.CTkButton(
            btn_frame,
            text="📄 Files",
            width=90,
            height=32,
            corner_radius=8,
            fg_color="#374151",
            hover_color="#4b5563",
            command=self._browse_files,
        ).pack(side="left")

        # File count label
        self.file_label = ctk.CTkLabel(
            content,
            text="No files selected",
            font=ctk.CTkFont(size=12),
            text_color="#6b7280",
        )
        self.file_label.grid(row=3, column=0, sticky="w", pady=(4, 0))

    def _browse_folder(self):
        from tkinter import filedialog

        folder = filedialog.askdirectory(title=f"Select folder for {self.name_var.get()}")
        if folder:
            files = list(Path(folder).glob("*.xlsx")) + list(Path(folder).glob("*.xls"))
            if files:
                self.selected_files = [str(f) for f in files]
                self.path_var.set(folder)
                self.file_label.configure(text=f"📁 {len(files)} Excel files", text_color="#10b981")
            else:
                self.file_label.configure(text="⚠️ No Excel files in folder", text_color="#eab308")

    def _browse_files(self):
        from tkinter import filedialog

        files = filedialog.askopenfilenames(
            title=f"Select Excel files for {self.name_var.get()}",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if files:
            self.selected_files = list(files)
            # Show first file's directory
            first_dir = str(Path(files[0]).parent)
            self.path_var.set(f"{first_dir} ({len(files)} files)")
            self.file_label.configure(text=f"📄 {len(files)} files selected", text_color="#10b981")

    def _remove(self):
        self.on_remove(self.card_id)

    def get_source(self) -> dict:
        return {
            "name": self.name_var.get(),
            "path": self.path_var.get(),
            "files": self.selected_files,
        }

    def set_source(self, name: str, path: str, files: list = None):
        self.name_var.set(name)
        self.path_var.set(path)
        if files:
            self.selected_files = files
            self.file_label.configure(text=f"📄 {len(files)} files", text_color="#10b981")

    def set_status(self, status: str):
        self.status = status
        color = self.STATUS_COLORS.get(status, "#6b7280")
        self.status_frame.configure(fg_color=color)


# ============================================================
# MAIN APPLICATION
# ============================================================


class DataPipelineApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Data Processing Pipeline")
        self.geometry("700x800")
        self.minsize(600, 700)

        self.source_cards = {}
        self.card_counter = 0
        self.dashboard_path = None

        self.setup_ui()

    def setup_ui(self):
        # Configure grid
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        # Header
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 10))

        ctk.CTkLabel(
            header,
            text="📊 Data Processing Pipeline",
            font=ctk.CTkFont(size=24, weight="bold"),
        ).pack(side="left")

        # Sources section header
        sources_header = ctk.CTkFrame(self, fg_color="transparent")
        sources_header.grid(row=1, column=0, sticky="ew", padx=20, pady=(10, 5))

        ctk.CTkLabel(
            sources_header,
            text="Data Sources",
            font=ctk.CTkFont(size=16, weight="bold"),
        ).pack(side="left")

        add_btn = ctk.CTkButton(
            sources_header,
            text="+ Add Source",
            width=120,
            height=32,
            corner_radius=8,
            fg_color="#10b981",
            hover_color="#059669",
            command=self.add_source,
        )
        add_btn.pack(side="right")

        # Scrollable sources container
        self.sources_scroll = ctk.CTkScrollableFrame(
            self,
            corner_radius=12,
            fg_color="#0f172a",
        )
        self.sources_scroll.grid(row=2, column=0, sticky="nsew", padx=20, pady=5)
        self.sources_scroll.grid_columnconfigure(0, weight=1)

        # Config & Output section
        config_frame = ctk.CTkFrame(self, fg_color="transparent")
        config_frame.grid(row=3, column=0, sticky="ew", padx=20, pady=10)
        config_frame.grid_columnconfigure(1, weight=1)

        # Config buttons
        btn_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        btn_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))

        ctk.CTkButton(
            btn_frame,
            text="💾 Save Config",
            width=110,
            height=32,
            corner_radius=8,
            fg_color="#374151",
            hover_color="#4b5563",
            command=self.save_config,
        ).pack(side="left", padx=(0, 8))

        ctk.CTkButton(
            btn_frame,
            text="📂 Load Config",
            width=110,
            height=32,
            corner_radius=8,
            fg_color="#374151",
            hover_color="#4b5563",
            command=self.load_config,
        ).pack(side="left")

        # Output folder
        ctk.CTkLabel(config_frame, text="Output Folder:", font=ctk.CTkFont(size=13)).grid(
            row=1, column=0, sticky="w", pady=(0, 5)
        )

        output_row = ctk.CTkFrame(config_frame, fg_color="transparent")
        output_row.grid(row=2, column=0, columnspan=2, sticky="ew")
        output_row.grid_columnconfigure(0, weight=1)

        self.output_var = ctk.StringVar(value=str(Path.home() / "DataPipeline" / "output"))
        self.output_entry = ctk.CTkEntry(output_row, textvariable=self.output_var, height=36)
        self.output_entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))

        ctk.CTkButton(
            output_row,
            text="Browse",
            width=80,
            height=36,
            corner_radius=8,
            command=self.browse_output,
        ).grid(row=0, column=1)

        # Run button
        self.run_btn = ctk.CTkButton(
            self,
            text="▶  Run Pipeline",
            height=50,
            corner_radius=12,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="#3b82f6",
            hover_color="#2563eb",
            command=self.run_pipeline,
        )
        self.run_btn.grid(row=4, column=0, sticky="ew", padx=20, pady=15)

        # Add initial source (must be after run_btn is created)
        self.add_source()

        # Progress section
        progress_frame = ctk.CTkFrame(self, fg_color="transparent")
        progress_frame.grid(row=5, column=0, sticky="nsew", padx=20, pady=(0, 10))
        progress_frame.grid_columnconfigure(0, weight=1)
        progress_frame.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(
            progress_frame,
            text="Progress",
            font=ctk.CTkFont(size=13),
        ).grid(row=0, column=0, sticky="w", pady=(0, 5))

        self.log_text = ctk.CTkTextbox(
            progress_frame,
            height=150,
            corner_radius=8,
            font=ctk.CTkFont(family="Consolas", size=12),
        )
        self.log_text.grid(row=1, column=0, sticky="nsew")

        # Dashboard button (hidden initially)
        self.dashboard_btn = ctk.CTkButton(
            self,
            text="📊 Open Dashboard",
            height=44,
            corner_radius=10,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#8b5cf6",
            hover_color="#7c3aed",
            command=self.open_dashboard,
        )

        # Configure row weights
        self.grid_rowconfigure(2, weight=1)
        self.grid_rowconfigure(5, weight=1)

    def add_source(self):
        self.card_counter += 1
        card = SourceCard(self.sources_scroll, self.remove_source, self.card_counter)
        card.pack(fill="x", pady=6)
        self.source_cards[self.card_counter] = card
        self.update_run_button()

    def remove_source(self, card_id):
        from tkinter import messagebox

        if len(self.source_cards) <= 1:
            messagebox.showwarning("Warning", "At least one source is required")
            return
        if card_id in self.source_cards:
            self.source_cards[card_id].destroy()
            del self.source_cards[card_id]
        self.update_run_button()

    def update_run_button(self):
        if not hasattr(self, "run_btn"):
            return
        count = len(self.source_cards)
        self.run_btn.configure(text=f"▶  Run Pipeline ({count} source{'s' if count > 1 else ''})")

    def browse_output(self):
        from tkinter import filedialog

        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.output_var.set(folder)

    def save_config(self):
        from tkinter import filedialog

        sources = [card.get_source() for card in self.source_cards.values()]
        config = {"sources": sources, "output_dir": self.output_var.get()}

        file_path = filedialog.asksaveasfilename(
            defaultextension=".yaml",
            filetypes=[("YAML files", "*.yaml")],
            title="Save Configuration",
        )
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, default_flow_style=False)
            self.log(f"✅ Config saved: {file_path}")

    def load_config(self):
        from tkinter import filedialog, messagebox

        file_path = filedialog.askopenfilename(
            filetypes=[("YAML files", "*.yaml")],
            title="Load Configuration",
        )
        if file_path:
            try:
                with open(file_path, encoding="utf-8") as f:
                    config = yaml.safe_load(f)

                for card_id in list(self.source_cards.keys()):
                    self.source_cards[card_id].destroy()
                self.source_cards.clear()

                for source in config.get("sources", []):
                    self.card_counter += 1
                    card = SourceCard(self.sources_scroll, self.remove_source, self.card_counter)
                    card.set_source(source.get("name", ""), source.get("path", ""))
                    card.pack(fill="x", pady=6)
                    self.source_cards[self.card_counter] = card

                if "output_dir" in config:
                    self.output_var.set(config["output_dir"])

                self.update_run_button()
                self.log(f"✅ Config loaded: {file_path}")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to load: {str(e)}")

    def log(self, message):
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.update_idletasks()

    def update_source_status(self, name, status):
        for card in self.source_cards.values():
            if card.name_var.get() == name:
                card.set_status(status)
                break
        self.update_idletasks()

    def run_pipeline(self):
        from tkinter import messagebox

        sources = []
        for card in self.source_cards.values():
            source = card.get_source()
            if not source["path"] or not os.path.isdir(source["path"]):
                messagebox.showerror("Error", f"Invalid path: {source['name']}")
                return
            sources.append(source)

        if not sources:
            messagebox.showerror("Error", "Add at least one source")
            return

        for card in self.source_cards.values():
            card.set_status("pending")

        self.run_btn.configure(state="disabled", text="⏳ Processing...")
        self.log_text.delete("1.0", "end")
        self.dashboard_btn.grid_forget()

        def task():
            try:
                config_path = resource_path("config.yaml")

                results = run_multi_source_etl(
                    sources=sources,
                    output_dir=self.output_var.get(),
                    config_path=config_path,
                    progress_callback=self.log,
                    source_callback=self.update_source_status,
                )

                if results["successful"]:
                    self.log("\n📊 Generating dashboard...")

                    try:
                        # Get dashboard data
                        dashboard_data = export_data_json(self.output_var.get())

                        # Deploy with embedded data (no CORS issues)
                        dashboard_html = deploy_react_dashboard(
                            self.output_var.get(), dashboard_data
                        )
                        if dashboard_html:
                            self.dashboard_path = dashboard_html
                            self.log("✅ Dashboard ready!")
                        else:
                            self.dashboard_path = None
                            self.log("⚠️ React dashboard not available")
                    except Exception as e:
                        self.dashboard_path = None
                        self.log(f"⚠️ Dashboard: {str(e)}")

                    self.after(
                        0, lambda: self.dashboard_btn.grid(row=6, column=0, padx=20, pady=(0, 15))
                    )

                if results["failed"]:
                    failed_count = len(results["failed"])
                    success_count = len(results["successful"])

                    if success_count > 0:
                        self.after(
                            0,
                            lambda: messagebox.showwarning(
                                "Partial Success",
                                f"✅ Success: {success_count}\n❌ Failed: {failed_count}\n\n"
                                + "\n".join(f"• {n}" for n in results["failed"]),
                            ),
                        )
                    else:
                        self.after(
                            0,
                            lambda: messagebox.showerror(
                                "Failed",
                                f"All {failed_count} source(s) failed:\n\n"
                                + "\n".join(f"• {n}" for n in results["failed"]),
                            ),
                        )

            except Exception as ex:
                import traceback

                error_msg = str(ex)
                self.log(f"\n❌ Error: {error_msg}")
                self.log(traceback.format_exc())
                self.after(0, lambda m=error_msg: messagebox.showerror("Error", m))
            finally:
                self.after(
                    0,
                    lambda: self.run_btn.configure(
                        state="normal", text=f"▶  Run Pipeline ({len(sources)} sources)"
                    ),
                )

        threading.Thread(target=task, daemon=True).start()

    def open_dashboard(self):
        """Open dashboard in browser via HTTP server."""
        if self.dashboard_path:
            try:
                server = get_dashboard_server(self.dashboard_path)
                url = server.get_url()
                self.log(f"🌐 Dashboard server: {url}")
                webbrowser.open(url)
            except Exception as e:
                self.log(f"⚠️ Server error: {e}, opening file directly")
                webbrowser.open(f"file://{self.dashboard_path}/index.html")


def main():
    app = DataPipelineApp()
    app.mainloop()


if __name__ == "__main__":
    main()
