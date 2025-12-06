# Building Windows Executable

This creates a standalone `.exe` file that runs without Python or any libraries installed.

The GUI app uses the **same ETL logic** as the CLI (`main.py`) via the shared `src/etl/pipeline.py` module.

## Option 1: Build on Windows (Recommended)

### Prerequisites
1. Install Python 3.10+ from https://python.org
2. Open Command Prompt

### Build Steps

```cmd
# 1. Install dependencies
pip install pyinstaller polars fastexcel openpyxl pyyaml

# 2. Run the build script
python build_exe.py

# 3. Find your exe
# Output: dist\ShopeeDataPipeline.exe
```

### Manual Build (alternative)
```cmd
pyinstaller --onefile --windowed --name DataProcessingPipeline ^
    --add-data "config.yaml;." ^
    --add-data "src;src" ^
    --hidden-import src.etl.pipeline ^
    --collect-all polars ^
    app_gui.py
```

## Option 2: Build on Mac for Windows (Cross-compile)

Not directly supported. Options:
1. Use a Windows VM
2. Use GitHub Actions (see below)
3. Use Wine + PyInstaller (complex)

## Option 3: GitHub Actions (Automated)

Create `.github/workflows/build.yml`:

```yaml
name: Build Windows EXE

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  build:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install pyinstaller polars fastexcel openpyxl pyyaml
      
      - name: Build EXE
        run: |
          python build_exe.py
      
      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: DataProcessingPipeline
          path: dist/DataProcessingPipeline.exe
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Shared Codebase                      │
├─────────────────────────────────────────────────────────┤
│                  src/etl/pipeline.py                    │
│            (run_simple_etl, generate_metrics)           │
└───────────────────┬─────────────────┬───────────────────┘
                    │                 │
          ┌─────────▼─────────┐  ┌────▼────┐
          │    app_gui.py     │  │ main.py │
          │  (tkinter GUI)    │  │  (CLI)  │
          └───────────────────┘  └─────────┘
                    │
          ┌─────────▼─────────┐
          │DataProcessingPipeline│
          │      .exe         │
          └───────────────────┘
```

## Output

After building, you'll have:
```
dist/
└── DataProcessingPipeline.exe   # ~50-100MB standalone executable
```

## Distributing

Just copy `DataProcessingPipeline.exe` to the target Windows machine. 
No installation required - just double-click to run!

## Troubleshooting

### "Windows protected your PC" / SmartScreen Warning

This is normal for unsigned apps. To run:

**Method 1:**
1. Click **"More info"**
2. Click **"Run anyway"**

**Method 2:**
1. Right-click the `.exe` file
2. Select **Properties**
3. At the bottom, check **"Unblock"**
4. Click **OK**
5. Double-click to run

**Method 3 (IT Admin):**
```powershell
# Unblock via PowerShell
Unblock-File -Path "DataProcessingPipeline*.exe"
```

### Missing DLL errors
- Make sure you built on same Windows version as target
- Try adding `--collect-all polars` to pyinstaller command

### Antivirus false positive
- Common with PyInstaller exes
- Add exception or sign the executable

### Import errors
- The `src/etl` modules are bundled with `--add-data "src;src"`
- Hidden imports are specified for each module
