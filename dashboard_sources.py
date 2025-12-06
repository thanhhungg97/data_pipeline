"""Multi-source comparison dashboard."""
import polars as pl
from pathlib import Path
import json


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files from all sources."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))
    
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")
    
    # Use diagonal concat to handle different schemas
    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_source_metrics(df: pl.DataFrame) -> list[dict]:
    """Calculate metrics grouped by Source, Year, Month."""
    metrics = df.group_by(["Source", "Year", "Month"]).agg([
        pl.len().alias("total_orders"),
        pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
        pl.col("Status").filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"])).len().alias("cancelled"),
        pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
        pl.col("Status").filter(pl.col("Status").is_in(["Failed delivery", "Failed"])).len().alias("failed"),
    ]).sort(["Source", "Year", "Month"])
    
    # Add rates
    metrics = metrics.with_columns([
        (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
        (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
    ])
    
    return metrics.to_dicts()


def generate_source_comparison_dashboard(
    df: pl.DataFrame, 
    output_file: str = "dashboard_sources.html"
):
    """Generate interactive HTML dashboard for comparing sources."""
    
    sources = df["Source"].unique().sort().to_list()
    metrics = calculate_source_metrics(df)
    
    html_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Multi-Source Comparison Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', system-ui, sans-serif; 
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #e0e0e0;
        }
        .header {
            background: rgba(255,255,255,0.05);
            padding: 20px 40px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 { font-size: 28px; font-weight: 600; color: #fff; }
        .header p { color: #888; margin-top: 5px; }
        
        .controls {
            padding: 20px 40px;
            display: flex;
            gap: 20px;
            align-items: flex-end;
            flex-wrap: wrap;
            background: rgba(255,255,255,0.02);
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .control-group { display: flex; flex-direction: column; gap: 5px; }
        .control-group label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; }
        select, button {
            padding: 10px 20px;
            font-size: 14px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            background: rgba(255,255,255,0.1);
            color: #fff;
            cursor: pointer;
        }
        select:hover, button:hover { border-color: #00d4aa; }
        button.active { background: #00d4aa; border-color: #00d4aa; }
        
        .source-chips {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .chip {
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 13px;
            cursor: pointer;
            transition: all 0.2s;
            border: 2px solid transparent;
        }
        .chip.selected { border-color: #fff; }
        .chip-shopee { background: #ee4d2d; }
        .chip-website { background: #4a90a4; }
        .chip-lazada { background: #0f146d; }
        .chip-default { background: #6c5ce7; }
        
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            padding: 20px 40px;
        }
        .source-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .source-card h3 {
            font-size: 18px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .source-badge {
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        .metric-row {
            display: flex;
            justify-content: space-between;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .metric-row:last-child { border-bottom: none; }
        .metric-label { color: #888; }
        .metric-value { font-weight: 600; font-size: 18px; }
        .positive { color: #00d4aa; }
        .negative { color: #ff6b6b; }
        
        .charts-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 20px;
            padding: 20px 40px;
        }
        .chart-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .chart-card h3 { margin-bottom: 15px; color: #fff; }
        
        .comparison-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 40px;
            max-width: calc(100% - 80px);
        }
        .comparison-table th, .comparison-table td {
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .comparison-table th {
            background: rgba(255,255,255,0.05);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 12px;
            letter-spacing: 1px;
            color: #888;
        }
        .comparison-table tr:hover { background: rgba(255,255,255,0.02); }
        .best { color: #00d4aa; font-weight: 600; }
        .worst { color: #ff6b6b; }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Multi-Source Comparison Dashboard</h1>
        <p>Compare performance across different sales channels</p>
    </div>
    
    <div class="controls">
        <div class="control-group">
            <label>Select Month</label>
            <select id="monthSelector" onchange="updateDashboard()"></select>
        </div>
        <div class="control-group">
            <label>Compare Sources</label>
            <div class="source-chips" id="sourceChips"></div>
        </div>
        <div class="control-group">
            <label>&nbsp;</label>
            <button onclick="selectAllSources()">Select All</button>
        </div>
    </div>
    
    <div class="summary-grid" id="summaryGrid"></div>
    
    <div class="charts-grid">
        <div class="chart-card">
            <h3>📈 Orders by Source Over Time</h3>
            <div id="trendChart"></div>
        </div>
        <div class="chart-card">
            <h3>📊 Source Comparison (Selected Month)</h3>
            <div id="comparisonChart"></div>
        </div>
    </div>
    
    <h3 style="padding: 20px 40px 10px; color: #fff;">📋 Detailed Comparison</h3>
    <table class="comparison-table" id="comparisonTable">
        <thead>
            <tr>
                <th>Metric</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>

    <script>
        const metricsData = METRICS_PLACEHOLDER;
        const allSources = SOURCES_PLACEHOLDER;
        let selectedSources = [...allSources];
        
        const sourceColors = {
            'Shopee': '#ee4d2d',
            'Website': '#4a90a4',
            'Lazada': '#0f146d',
            'Tiki': '#1a94ff',
            'default': '#6c5ce7'
        };
        
        const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        
        // Get unique year-month combinations
        const periods = [...new Set(metricsData.map(d => `${d.Year}-${d.Month}`))].sort().reverse();
        
        // Populate month selector
        const selector = document.getElementById('monthSelector');
        periods.forEach((p, i) => {
            const [year, month] = p.split('-').map(Number);
            const opt = document.createElement('option');
            opt.value = p;
            opt.textContent = `${monthNames[month]} ${year}`;
            if (i === 0) opt.selected = true;
            selector.appendChild(opt);
        });
        
        // Populate source chips
        function renderSourceChips() {
            const container = document.getElementById('sourceChips');
            container.innerHTML = '';
            allSources.forEach(source => {
                const chip = document.createElement('div');
                const colorClass = sourceColors[source] ? `chip-${source.toLowerCase()}` : 'chip-default';
                chip.className = `chip ${colorClass} ${selectedSources.includes(source) ? 'selected' : ''}`;
                chip.style.background = sourceColors[source] || sourceColors.default;
                chip.textContent = source;
                chip.onclick = () => toggleSource(source);
                container.appendChild(chip);
            });
        }
        
        function toggleSource(source) {
            if (selectedSources.includes(source)) {
                if (selectedSources.length > 1) {
                    selectedSources = selectedSources.filter(s => s !== source);
                }
            } else {
                selectedSources.push(source);
            }
            renderSourceChips();
            updateDashboard();
        }
        
        function selectAllSources() {
            selectedSources = [...allSources];
            renderSourceChips();
            updateDashboard();
        }
        
        function getSourceData(source, year, month) {
            return metricsData.find(d => d.Source === source && d.Year === year && d.Month === month);
        }
        
        function fmt(n) { return n ? n.toLocaleString() : '0'; }
        
        function updateDashboard() {
            const [year, month] = selector.value.split('-').map(Number);
            
            // Update summary cards
            const grid = document.getElementById('summaryGrid');
            grid.innerHTML = '';
            
            selectedSources.forEach(source => {
                const data = getSourceData(source, year, month);
                const color = sourceColors[source] || sourceColors.default;
                
                const card = document.createElement('div');
                card.className = 'source-card';
                card.innerHTML = `
                    <h3>
                        <span class="source-badge" style="background: ${color}">${source}</span>
                        ${monthNames[month]} ${year}
                    </h3>
                    <div class="metric-row">
                        <span class="metric-label">Total Orders</span>
                        <span class="metric-value">${data ? fmt(data.total_orders) : '-'}</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">Delivered</span>
                        <span class="metric-value positive">${data ? data.delivery_rate + '%' : '-'}</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">Cancelled</span>
                        <span class="metric-value negative">${data ? data.cancel_rate + '%' : '-'}</span>
                    </div>
                    <div class="metric-row">
                        <span class="metric-label">Returned</span>
                        <span class="metric-value">${data ? fmt(data.returned) : '-'}</span>
                    </div>
                `;
                grid.appendChild(card);
            });
            
            updateCharts(year, month);
            updateComparisonTable(year, month);
        }
        
        function updateCharts(year, month) {
            // Trend chart - last 6 months for each selected source
            const traces = selectedSources.map(source => {
                const sourceData = metricsData
                    .filter(d => d.Source === source)
                    .sort((a, b) => a.Year !== b.Year ? a.Year - b.Year : a.Month - b.Month)
                    .slice(-12);
                
                return {
                    x: sourceData.map(d => `${monthNames[d.Month]} ${d.Year}`),
                    y: sourceData.map(d => d.total_orders),
                    type: 'scatter',
                    mode: 'lines+markers',
                    name: source,
                    line: { color: sourceColors[source] || sourceColors.default, width: 3 },
                    marker: { size: 8 }
                };
            });
            
            Plotly.newPlot('trendChart', traces, {
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: { color: '#888' },
                margin: { t: 20, r: 20, b: 60, l: 60 },
                xaxis: { gridcolor: 'rgba(255,255,255,0.1)', tickangle: -45 },
                yaxis: { gridcolor: 'rgba(255,255,255,0.1)', title: 'Orders' },
                legend: { orientation: 'h', y: 1.1 },
                showlegend: true
            }, {responsive: true});
            
            // Comparison bar chart
            const metrics = ['Total Orders', 'Delivered', 'Cancelled', 'Returned'];
            const barTraces = selectedSources.map(source => {
                const data = getSourceData(source, year, month);
                return {
                    x: metrics,
                    y: data ? [data.total_orders, data.delivered, data.cancelled, data.returned] : [0,0,0,0],
                    type: 'bar',
                    name: source,
                    marker: { color: sourceColors[source] || sourceColors.default }
                };
            });
            
            Plotly.newPlot('comparisonChart', barTraces, {
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: { color: '#888' },
                margin: { t: 20, r: 20, b: 40, l: 60 },
                barmode: 'group',
                legend: { orientation: 'h', y: 1.1 },
                yaxis: { gridcolor: 'rgba(255,255,255,0.1)' }
            }, {responsive: true});
        }
        
        function updateComparisonTable(year, month) {
            const table = document.getElementById('comparisonTable');
            const thead = table.querySelector('thead tr');
            const tbody = table.querySelector('tbody');
            
            // Update header
            thead.innerHTML = '<th>Metric</th>' + selectedSources.map(s => 
                `<th style="color: ${sourceColors[s] || sourceColors.default}">${s}</th>`
            ).join('');
            
            // Get data for each source
            const sourceData = selectedSources.map(s => getSourceData(s, year, month) || {});
            
            // Metrics to compare
            const rows = [
                { label: 'Total Orders', key: 'total_orders', best: 'max' },
                { label: 'Delivered', key: 'delivered', best: 'max' },
                { label: 'Delivery Rate', key: 'delivery_rate', best: 'max', suffix: '%' },
                { label: 'Cancelled', key: 'cancelled', best: 'min' },
                { label: 'Cancel Rate', key: 'cancel_rate', best: 'min', suffix: '%' },
                { label: 'Returned', key: 'returned', best: 'min' },
                { label: 'Failed', key: 'failed', best: 'min' },
            ];
            
            tbody.innerHTML = rows.map(row => {
                const values = sourceData.map(d => d[row.key] || 0);
                const best = row.best === 'max' ? Math.max(...values) : Math.min(...values.filter(v => v > 0));
                
                return `<tr>
                    <td>${row.label}</td>
                    ${values.map(v => {
                        const isBest = v === best && v > 0;
                        const cls = isBest ? 'best' : '';
                        return `<td class="${cls}">${fmt(v)}${row.suffix || ''}</td>`;
                    }).join('')}
                </tr>`;
            }).join('');
        }
        
        // Initialize
        renderSourceChips();
        updateDashboard();
    </script>
</body>
</html>'''
    
    # Replace placeholders
    html_content = html_content.replace('METRICS_PLACEHOLDER', json.dumps(metrics))
    html_content = html_content.replace('SOURCES_PLACEHOLDER', json.dumps(sources))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n✅ Source comparison dashboard saved to: {output_file}")
    return output_file


if __name__ == "__main__":
    df = load_all_data()
    
    sources = df["Source"].unique().to_list()
    print(f"\nSources found: {sources}")
    
    generate_source_comparison_dashboard(df)

