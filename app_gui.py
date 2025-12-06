"""
Standalone GUI app for Data Processing Pipeline.
Supports multiple sources with custom names and paths.
Uses only tkinter (built into Python) - no extra GUI dependencies.
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

import yaml


# Handle PyInstaller bundled paths
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and PyInstaller."""
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# ============================================================
# IMPORT SHARED ETL LOGIC
# ============================================================

sys.path.insert(0, resource_path("."))

from src.etl.pipeline import run_simple_etl  # noqa: E402

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
    """Run ETL for multiple sources with error handling.

    Args:
        sources: List of {"name": str, "path": str}
        output_dir: Base output directory
        config_path: Path to config.yaml
        progress_callback: Callback for log messages
        source_callback: Callback for source status updates (name, status)

    Returns:
        Dict with results including errors
    """
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

    # Track results
    results = {
        "successful": [],
        "failed": [],
        "errors": {},
        "total_rows": 0,
    }

    # Verify config exists
    if not os.path.exists(config_path):
        log(f"⚠️ Config not found at {config_path}, using defaults")

    log(f"🚀 Starting pipeline for {len(sources)} source(s)...")

    # Process each source
    for i, source in enumerate(sources, 1):
        name = source["name"]
        path = source["path"]

        update_source(name, "processing")
        log(f"\n{'=' * 50}")
        log(f"📂 [{i}/{len(sources)}] Processing: {name}")
        log(f"   Path: {path}")

        try:
            # Validate path
            if not os.path.isdir(path):
                raise FileNotFoundError(f"Directory not found: {path}")

            # Check for Excel files
            excel_files = list(Path(path).glob("*.xlsx")) + list(Path(path).glob("*.xls"))
            if not excel_files:
                raise FileNotFoundError(f"No Excel files found in: {path}")

            log(f"   Found {len(excel_files)} Excel file(s)")

            # Run ETL for this source
            source_output = output_path / name.lower().replace(" ", "_")
            run_simple_etl(
                input_dir=path,
                output_dir=str(source_output),
                config_path=config_path,
                progress_callback=log,
            )

            # Load processed data for combining
            parquet_files = list(source_output.glob("**/*.parquet"))
            if parquet_files:
                df = pl.concat([pl.read_parquet(f) for f in parquet_files])
                df = df.with_columns(pl.lit(name).alias("Source"))
                all_dataframes.append(df)
                rows = len(df)
            else:
                rows = 0

            update_source(name, "done")
            log(f"✅ {name}: {rows:,} rows processed successfully")

            results["successful"].append(name)
            results["total_rows"] += rows

        except FileNotFoundError as e:
            update_source(name, "error")
            error_msg = str(e)
            log(f"❌ {name}: {error_msg}")
            results["failed"].append(name)
            results["errors"][name] = {"type": "FileNotFound", "message": error_msg}

        except Exception as e:
            update_source(name, "error")
            error_msg = str(e)
            error_trace = traceback.format_exc()
            log(f"❌ {name}: Error - {error_msg}")
            log(f"   Details: {error_trace.split(chr(10))[-2]}")
            results["failed"].append(name)
            results["errors"][name] = {
                "type": type(e).__name__,
                "message": error_msg,
                "traceback": error_trace,
            }

    # Combine all sources
    if all_dataframes:
        log(f"\n{'=' * 50}")
        log("🔄 Combining all sources...")

        combined = pl.concat(all_dataframes, how="diagonal")

        # Save combined data
        combined_output = output_path / "all_sources"
        combined_output.mkdir(parents=True, exist_ok=True)

        # Partition by Year/Month
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

        log(f"✅ Combined {len(combined):,} total rows from {len(results['successful'])} sources")

    # Print summary
    log(f"\n{'=' * 50}")
    log("📊 PIPELINE SUMMARY")
    log(f"{'=' * 50}")
    log(f"   ✅ Successful: {len(results['successful'])} source(s)")
    for name in results["successful"]:
        log(f"      • {name}")

    if results["failed"]:
        log(f"   ❌ Failed: {len(results['failed'])} source(s)")
        for name in results["failed"]:
            error = results["errors"].get(name, {})
            log(f"      • {name}: {error.get('message', 'Unknown error')}")

    log(f"   📁 Total rows: {results['total_rows']:,}")
    log(f"   📂 Output: {output_path}")

    results["output_dir"] = str(output_path)
    return results


# ============================================================
# DASHBOARD
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

    # Get sources
    sources = []
    if "Source" in df.columns:
        sources = sorted(df["Source"].unique().to_list())

    # Monthly metrics
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

    # Per-source metrics
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

    # Build JSON
    data = {
        "monthly": monthly.to_dicts(),
        "metrics": metrics_data,
        "sources": sources,
        "reasons": [],
    }

    output_path = Path(data_dir) / "dashboard" / "data.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    return str(output_path)


def deploy_react_dashboard(output_dir: str) -> str:
    """Copy bundled React dashboard to output directory."""
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
    return str(dest_dir / "index.html")


# ============================================================
# SOURCE CARD WIDGET
# ============================================================


class SourceCard(tk.Frame):
    """A card widget for configuring a single source."""

    def __init__(self, parent, on_remove, card_id):
        super().__init__(parent, bg="#16213e", relief="groove", bd=1)
        self.on_remove = on_remove
        self.card_id = card_id
        self.status = "pending"

        # Header with remove button
        header = tk.Frame(self, bg="#16213e")
        header.pack(fill="x", padx=10, pady=(10, 5))

        self.status_label = tk.Label(
            header, text="⏳", bg="#16213e", fg="#888", font=("Segoe UI", 12)
        )
        self.status_label.pack(side="left")

        remove_btn = tk.Button(
            header,
            text="✕",
            command=self._remove,
            bg="#16213e",
            fg="#ff6b6b",
            font=("Segoe UI", 10, "bold"),
            bd=0,
            cursor="hand2",
        )
        remove_btn.pack(side="right")

        # Source name
        name_frame = tk.Frame(self, bg="#16213e")
        name_frame.pack(fill="x", padx=10, pady=2)

        tk.Label(name_frame, text="Name:", bg="#16213e", fg="#888", font=("Segoe UI", 9)).pack(
            side="left"
        )

        self.name_var = tk.StringVar(value=f"Source {card_id}")
        self.name_entry = tk.Entry(
            name_frame,
            textvariable=self.name_var,
            font=("Segoe UI", 10),
            width=20,
            bg="#1a1a2e",
            fg="white",
            insertbackground="white",
        )
        self.name_entry.pack(side="left", padx=(5, 0), fill="x", expand=True)

        # Folder path
        path_frame = tk.Frame(self, bg="#16213e")
        path_frame.pack(fill="x", padx=10, pady=(2, 5))

        tk.Label(path_frame, text="Folder:", bg="#16213e", fg="#888", font=("Segoe UI", 9)).pack(
            side="left"
        )

        self.path_var = tk.StringVar()
        self.path_entry = tk.Entry(
            path_frame,
            textvariable=self.path_var,
            font=("Segoe UI", 9),
            width=30,
            bg="#1a1a2e",
            fg="white",
            insertbackground="white",
        )
        self.path_entry.pack(side="left", padx=(5, 5), fill="x", expand=True)

        browse_btn = tk.Button(
            path_frame,
            text="Browse",
            command=self._browse,
            font=("Segoe UI", 8),
            bg="#4a90a4",
            fg="white",
        )
        browse_btn.pack(side="right")

        # File count
        self.file_label = tk.Label(
            self, text="No folder selected", bg="#16213e", fg="#666", font=("Segoe UI", 8)
        )
        self.file_label.pack(anchor="w", padx=10, pady=(0, 10))

        # Bind path change
        self.path_var.trace_add("write", self._on_path_change)

    def _browse(self):
        folder = filedialog.askdirectory(title=f"Select folder for {self.name_var.get()}")
        if folder:
            self.path_var.set(folder)

    def _on_path_change(self, *args):
        path = self.path_var.get()
        if path and os.path.isdir(path):
            files = list(Path(path).glob("*.xlsx")) + list(Path(path).glob("*.xls"))
            self.file_label.config(text=f"📁 {len(files)} Excel files found", fg="#00d4aa")
        else:
            self.file_label.config(text="No folder selected", fg="#666")

    def _remove(self):
        self.on_remove(self.card_id)

    def get_source(self) -> dict:
        """Return source config."""
        return {"name": self.name_var.get(), "path": self.path_var.get()}

    def set_source(self, name: str, path: str):
        """Set source config."""
        self.name_var.set(name)
        self.path_var.set(path)

    def set_status(self, status: str):
        """Update status indicator."""
        self.status = status
        icons = {"pending": "⏳", "processing": "🔄", "done": "✅", "error": "❌"}
        colors = {"pending": "#888", "processing": "#ffa726", "done": "#00d4aa", "error": "#ff6b6b"}
        self.status_label.config(text=icons.get(status, "⏳"), fg=colors.get(status, "#888"))


# ============================================================
# MAIN APPLICATION
# ============================================================


class DataPipelineApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Processing Pipeline")
        self.root.geometry("650x700")
        self.root.configure(bg="#1a1a2e")

        self.source_cards = {}
        self.card_counter = 0
        self.dashboard_path = None

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
        title.pack(pady=(15, 10))

        # Sources section
        sources_header = tk.Frame(self.root, bg="#1a1a2e")
        sources_header.pack(fill="x", padx=20)

        tk.Label(
            sources_header,
            text="Configure Sources:",
            bg="#1a1a2e",
            fg="#888",
            font=("Segoe UI", 10),
        ).pack(side="left")

        add_btn = tk.Button(
            sources_header,
            text="+ Add Source",
            command=self.add_source,
            font=("Segoe UI", 9),
            bg="#00d4aa",
            fg="white",
            cursor="hand2",
        )
        add_btn.pack(side="right")

        # Sources container (scrollable)
        self.sources_frame = tk.Frame(self.root, bg="#1a1a2e")
        self.sources_frame.pack(fill="both", expand=True, padx=20, pady=10)

        # Add initial source
        self.add_source()

        # Config buttons
        config_frame = tk.Frame(self.root, bg="#1a1a2e")
        config_frame.pack(fill="x", padx=20, pady=5)

        tk.Button(
            config_frame,
            text="💾 Save Config",
            command=self.save_config,
            font=("Segoe UI", 9),
            bg="#4a90a4",
            fg="white",
        ).pack(side="left", padx=(0, 5))

        tk.Button(
            config_frame,
            text="📂 Load Config",
            command=self.load_config,
            font=("Segoe UI", 9),
            bg="#4a90a4",
            fg="white",
        ).pack(side="left")

        # Output folder
        output_frame = tk.Frame(self.root, bg="#1a1a2e")
        output_frame.pack(fill="x", padx=20, pady=10)

        tk.Label(
            output_frame, text="Output Folder:", bg="#1a1a2e", fg="#888", font=("Segoe UI", 10)
        ).pack(anchor="w")

        output_row = tk.Frame(output_frame, bg="#1a1a2e")
        output_row.pack(fill="x", pady=5)

        self.output_var = tk.StringVar(value=str(Path.home() / "DataPipeline" / "processed"))
        self.output_entry = tk.Entry(
            output_row,
            textvariable=self.output_var,
            font=("Segoe UI", 10),
            bg="#16213e",
            fg="white",
            insertbackground="white",
        )
        self.output_entry.pack(side="left", fill="x", expand=True)

        tk.Button(
            output_row,
            text="Browse",
            command=self.browse_output,
            font=("Segoe UI", 9),
            bg="#4a90a4",
            fg="white",
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
        self.run_btn.pack(pady=15)

        # Progress log
        log_frame = tk.Frame(self.root, bg="#1a1a2e")
        log_frame.pack(fill="both", expand=True, padx=20, pady=(0, 10))

        tk.Label(log_frame, text="Progress:", bg="#1a1a2e", fg="#888", font=("Segoe UI", 10)).pack(
            anchor="w"
        )

        self.log_text = tk.Text(
            log_frame,
            height=8,
            bg="#16213e",
            fg="#e0e0e0",
            font=("Consolas", 9),
            wrap="word",
        )
        self.log_text.pack(fill="both", expand=True, pady=5)

        # Dashboard button
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

    def add_source(self):
        """Add a new source card."""
        self.card_counter += 1
        card = SourceCard(self.sources_frame, self.remove_source, self.card_counter)
        card.pack(fill="x", pady=5)
        self.source_cards[self.card_counter] = card
        self.update_run_button()

    def remove_source(self, card_id):
        """Remove a source card."""
        if len(self.source_cards) <= 1:
            messagebox.showwarning("Warning", "At least one source is required")
            return
        if card_id in self.source_cards:
            self.source_cards[card_id].destroy()
            del self.source_cards[card_id]
        self.update_run_button()

    def update_run_button(self):
        """Update run button text with source count."""
        count = len(self.source_cards)
        self.run_btn.config(text=f"▶ Run Pipeline ({count} source{'s' if count > 1 else ''})")

    def browse_output(self):
        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.output_var.set(folder)

    def save_config(self):
        """Save current configuration to file."""
        sources = [card.get_source() for card in self.source_cards.values()]
        config = {"sources": sources, "output_dir": self.output_var.get()}

        file_path = filedialog.asksaveasfilename(
            defaultextension=".yaml",
            filetypes=[("YAML files", "*.yaml"), ("All files", "*.*")],
            title="Save Configuration",
        )
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, default_flow_style=False)
            self.log(f"✅ Config saved to: {file_path}")

    def load_config(self):
        """Load configuration from file."""
        file_path = filedialog.askopenfilename(
            filetypes=[("YAML files", "*.yaml"), ("All files", "*.*")],
            title="Load Configuration",
        )
        if file_path:
            try:
                with open(file_path, encoding="utf-8") as f:
                    config = yaml.safe_load(f)

                # Clear existing sources
                for card_id in list(self.source_cards.keys()):
                    self.source_cards[card_id].destroy()
                self.source_cards.clear()

                # Load sources
                for source in config.get("sources", []):
                    self.card_counter += 1
                    card = SourceCard(self.sources_frame, self.remove_source, self.card_counter)
                    card.set_source(source.get("name", ""), source.get("path", ""))
                    card.pack(fill="x", pady=5)
                    self.source_cards[self.card_counter] = card

                # Load output dir
                if "output_dir" in config:
                    self.output_var.set(config["output_dir"])

                self.update_run_button()
                self.log(f"✅ Config loaded from: {file_path}")

            except Exception as e:
                messagebox.showerror("Error", f"Failed to load config: {str(e)}")

    def log(self, message):
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.root.update_idletasks()

    def update_source_status(self, name, status):
        """Update status for a source card by name."""
        for card in self.source_cards.values():
            if card.name_var.get() == name:
                card.set_status(status)
                break
        self.root.update_idletasks()

    def run_pipeline(self):
        # Validate sources
        sources = []
        for card in self.source_cards.values():
            source = card.get_source()
            if not source["path"] or not os.path.isdir(source["path"]):
                messagebox.showerror("Error", f"Invalid path for source: {source['name']}")
                return
            sources.append(source)

        if not sources:
            messagebox.showerror("Error", "Please add at least one source")
            return

        # Reset status
        for card in self.source_cards.values():
            card.set_status("pending")

        self.run_btn.config(state="disabled", text="Running...")
        self.log_text.delete("1.0", "end")
        self.dashboard_btn.pack_forget()

        def task():
            try:
                config_path = resource_path("config.yaml")

                # Run multi-source ETL
                results = run_multi_source_etl(
                    sources=sources,
                    output_dir=self.output_var.get(),
                    config_path=config_path,
                    progress_callback=self.log,
                    source_callback=self.update_source_status,
                )

                # Check if any sources succeeded
                if results["successful"]:
                    # Generate dashboard
                    self.log("\n📊 Generating dashboard...")

                    # Export data.json
                    try:
                        export_data_json(self.output_var.get())
                    except Exception as e:
                        self.log(f"⚠️ Dashboard export warning: {str(e)}")

                    # Deploy React dashboard
                    try:
                        dashboard_html = deploy_react_dashboard(self.output_var.get())
                        if dashboard_html:
                            self.dashboard_path = dashboard_html
                            self.log(f"✅ Dashboard ready: {dashboard_html}")
                        else:
                            self.dashboard_path = None
                            self.log("⚠️ React dashboard not bundled, data exported to JSON")
                    except Exception as e:
                        self.dashboard_path = None
                        self.log(f"⚠️ Dashboard deploy warning: {str(e)}")

                    # Show dashboard button
                    self.root.after(0, lambda: self.dashboard_btn.pack(pady=10))

                # Show error dialog if some sources failed
                if results["failed"]:
                    failed_count = len(results["failed"])
                    success_count = len(results["successful"])

                    if success_count > 0:
                        # Partial success
                        self.root.after(
                            0,
                            lambda: messagebox.showwarning(
                                "Partial Success",
                                f"Pipeline completed with errors:\n\n"
                                f"✅ Successful: {success_count} source(s)\n"
                                f"❌ Failed: {failed_count} source(s)\n\n"
                                f"Failed sources:\n"
                                + "\n".join(
                                    f"• {name}: {results['errors'][name]['message']}"
                                    for name in results["failed"]
                                ),
                            ),
                        )
                    else:
                        # All failed
                        self.root.after(
                            0,
                            lambda: messagebox.showerror(
                                "Pipeline Failed",
                                f"All {failed_count} source(s) failed:\n\n"
                                + "\n".join(
                                    f"• {name}: {results['errors'][name]['message']}"
                                    for name in results["failed"]
                                ),
                            ),
                        )

            except Exception as ex:
                import traceback

                error_trace = traceback.format_exc()
                error_msg = str(ex)
                self.log(f"\n❌ Critical Error: {error_msg}")
                self.log(f"   {error_trace}")
                self.root.after(
                    0,
                    lambda msg=error_msg: messagebox.showerror(
                        "Critical Error", f"Pipeline crashed:\n\n{msg}"
                    ),
                )
            finally:
                self.root.after(
                    0,
                    lambda: self.run_btn.config(
                        state="normal", text=f"▶ Run Pipeline ({len(sources)} sources)"
                    ),
                )

        threading.Thread(target=task, daemon=True).start()

    def open_dashboard(self):
        if self.dashboard_path:
            webbrowser.open(f"file://{self.dashboard_path}")
        else:
            # Open output folder
            webbrowser.open(f"file://{self.output_var.get()}")


def main():
    root = tk.Tk()
    DataPipelineApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
