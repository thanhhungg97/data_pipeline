# Building Windows Executable

This creates a standalone `.exe` file that runs without Python or any libraries installed.

## Option 1: Build on Windows (Recommended)

### Prerequisites
1. Install Python 3.10+ from https://python.org
2. Open Command Prompt

### Build Steps

```cmd
# 1. Install dependencies
pip install pyinstaller polars fastexcel openpyxl

# 2. Build the executable
pyinstaller --onefile --windowed --name ShopeeDataPipeline app_gui.py

# 3. Find your exe
# Output: dist\ShopeeDataPipeline.exe
```

### Alternative using spec file (more control)
```cmd
pyinstaller ShopeeDataPipeline.spec
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
          pip install pyinstaller polars fastexcel openpyxl
      
      - name: Build EXE
        run: |
          pyinstaller --onefile --windowed --name ShopeeDataPipeline app_gui.py
      
      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: ShopeeDataPipeline
          path: dist/ShopeeDataPipeline.exe
```

## Output

After building, you'll have:
```
dist/
└── ShopeeDataPipeline.exe   # ~50-100MB standalone executable
```

## Distributing

Just copy `ShopeeDataPipeline.exe` to the target Windows machine. 
No installation required - just double-click to run!

## Troubleshooting

### "Windows protected your PC" warning
- Click "More info" → "Run anyway"
- Or right-click exe → Properties → Unblock

### Missing DLL errors
- Make sure you built on same Windows version as target
- Try adding `--collect-all polars` to pyinstaller command

### Antivirus false positive
- Common with PyInstaller exes
- Add exception or sign the executable

