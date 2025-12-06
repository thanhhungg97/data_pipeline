"""
Build script to create standalone Windows executable.

Run this on a Windows machine to create the .exe:
    pip install pyinstaller
    python build_exe.py

Or use GitHub Actions for automated builds.
"""

import subprocess
import sys
from pathlib import Path


def build():
    # Install PyInstaller if not present
    subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"], check=True)

    # Check if React dashboard is built
    dashboard_dist = Path("dashboard/dist")
    if not dashboard_dist.exists():
        print("\n⚠️  React dashboard not built!")
        print("   Run: cd dashboard && npm run build")
        print("   Or the exe will use the simple HTML fallback.\n")
        dashboard_args = []
    else:
        print("\n✅ React dashboard found, will be bundled.\n")
        # Use dashboard_dist as the bundled name (different from source)
        dashboard_args = [
            "--add-data",
            "dashboard/dist;dashboard_dist",  # Use ; on Windows
        ]

    # Build the executable
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--onefile",  # Single .exe file
        "--windowed",  # No console window (for GUI app)
        "--name",
        "DataProcessingPipeline",  # Output name
        # Include config file
        "--add-data",
        "config.yaml;.",  # Use ; on Windows, : on Mac/Linux
        # Include src/etl package
        "--add-data",
        "src;src",  # Include entire src directory
        *dashboard_args,  # Include React dashboard if available
        # Hidden imports for the ETL modules
        "--hidden-import",
        "src.etl",
        "--hidden-import",
        "src.etl.pipeline",
        "--hidden-import",
        "src.etl.base",
        "--hidden-import",
        "src.etl.bronze",
        "--hidden-import",
        "src.etl.bronze.base",
        "--hidden-import",
        "src.etl.bronze.shopee",
        "--hidden-import",
        "src.etl.bronze.website",
        "--hidden-import",
        "src.etl.silver",
        "--hidden-import",
        "src.etl.silver.base",
        "--hidden-import",
        "src.etl.silver.shopee",
        "--hidden-import",
        "src.etl.silver.website",
        "--hidden-import",
        "src.etl.gold",
        "--hidden-import",
        "src.etl.gold.combine",
        "--hidden-import",
        "yaml",
        # Collect all polars dependencies
        "--collect-all",
        "polars",
        # Entry point
        "app_gui.py",
    ]

    subprocess.run(cmd, check=True)

    print("\n✅ Build complete! Find your .exe in the 'dist' folder")


if __name__ == "__main__":
    build()
