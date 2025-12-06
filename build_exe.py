"""
Build script to create standalone Windows executable.

Run this on a Windows machine to create the .exe:
    pip install pyinstaller
    python build_exe.py

Or from Mac, install Windows cross-compile tools.
"""
import subprocess
import sys

def build():
    # Install PyInstaller if not present
    subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"], check=True)
    
    # Build the executable
    subprocess.run([
        sys.executable, "-m", "PyInstaller",
        "--onefile",                    # Single .exe file
        "--windowed",                   # No console window (for GUI app)
        "--name", "ShopeeDataPipeline", # Output name
        "--add-data", "config.yaml;.",  # Include config (use ; on Windows, : on Mac/Linux)
        "app_gui.py"                    # Entry point
    ], check=True)
    
    print("\n✅ Build complete! Find your .exe in the 'dist' folder")

if __name__ == "__main__":
    build()

