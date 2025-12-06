"""Interactive dashboard with month-over-month and year-over-year comparisons."""
import polars as pl
from pathlib import Path


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files (supports multi-source structure)."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))
    
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")
    
    # Use diagonal concat to handle different schemas across sources
    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate monthly metrics."""
    monthly = df.group_by(["Year", "Month"]).agg([
        pl.len().alias("total_orders"),
        pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
        pl.col("Status").filter(pl.col("Status") == "Cancel by cust.").len().alias("cancelled"),
        pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
        pl.col("Status").filter(pl.col("Status") == "Failed delivery").len().alias("failed"),
    ]).sort(["Year", "Month"])
    
    # Add delivery rate
    monthly = monthly.with_columns([
        (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
        (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
    ])
    
    return monthly


def generate_html_dashboard(df: pl.DataFrame, output_file: str = "dashboard_compare.html"):
    """Generate interactive HTML dashboard with comparisons."""
    
    monthly = calculate_metrics(df)
    
    # Convert to list of dicts for JavaScript
    data_js = monthly.to_dicts()
    
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shopee Orders - Monthly Comparison Dashboard</title>
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
        .header h1 { 
            font-size: 28px; 
            font-weight: 600;
            color: #fff;
        }
        .header p { color: #888; margin-top: 5px; }
        
        .controls {
            padding: 20px 40px;
            display: flex;
            gap: 20px;
            align-items: center;
            flex-wrap: wrap;
        }
        .control-group {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .control-group label {
            font-size: 12px;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        select {
            padding: 10px 20px;
            font-size: 16px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            background: rgba(255,255,255,0.1);
            color: #fff;
            cursor: pointer;
            min-width: 150px;
        }
        select:hover { border-color: #00d4aa; }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            padding: 20px 40px;
        }
        .metric-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .metric-card h3 {
            font-size: 14px;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        .metric-value {
            font-size: 36px;
            font-weight: 700;
            color: #fff;
        }
        .comparisons {
            display: flex;
            gap: 20px;
            margin-top: 15px;
            flex-wrap: wrap;
        }
        .comparison {
            display: flex;
            flex-direction: column;
            gap: 2px;
        }
        .comparison-label {
            font-size: 11px;
            color: #666;
        }
        .comparison-value {
            font-size: 14px;
            font-weight: 600;
        }
        .positive { color: #00d4aa; }
        .negative { color: #ff6b6b; }
        .neutral { color: #888; }
        
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 20px;
            padding: 20px 40px;
        }
        .chart-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 20px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .chart-card h3 {
            font-size: 16px;
            margin-bottom: 15px;
            color: #fff;
        }
        
        .status-breakdown {
            padding: 20px 40px;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        .status-item {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 16px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .status-item h4 { 
            font-size: 13px; 
            color: #888; 
            margin-bottom: 8px;
        }
        .status-item .pct { 
            font-size: 28px; 
            font-weight: 700;
        }
        .status-item .value { 
            font-size: 14px; 
            color: #888;
            margin-left: 8px;
        }
        .delivered { color: #00d4aa; }
        .cancelled { color: #ff6b6b; }
        .returned { color: #ffa726; }
        .failed { color: #ab47bc; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🛒 Shopee Orders Dashboard</h1>
        <p>Monthly comparison with previous month and year-over-year</p>
    </div>
    
    <div class="controls">
        <div class="control-group">
            <label>Select Month</label>
            <select id="monthSelector" onchange="updateDashboard()"></select>
        </div>
    </div>
    
    <div class="metrics-grid">
        <div class="metric-card">
            <h3>Total Orders</h3>
            <div class="metric-value" id="totalOrders">-</div>
            <div class="comparisons">
                <div class="comparison">
                    <span class="comparison-label">vs Last Month</span>
                    <span class="comparison-value" id="totalVsLastMonth">-</span>
                </div>
                <div class="comparison">
                    <span class="comparison-label">vs Last Year</span>
                    <span class="comparison-value" id="totalVsLastYear">-</span>
                </div>
            </div>
        </div>
        
        <div class="metric-card">
            <h3>Delivery Rate</h3>
            <div class="metric-value" id="deliveryRate">-</div>
            <div class="comparisons">
                <div class="comparison">
                    <span class="comparison-label">vs Last Month</span>
                    <span class="comparison-value" id="deliveryVsLastMonth">-</span>
                </div>
                <div class="comparison">
                    <span class="comparison-label">vs Last Year</span>
                    <span class="comparison-value" id="deliveryVsLastYear">-</span>
                </div>
            </div>
        </div>
        
        <div class="metric-card">
            <h3>Cancellation Rate</h3>
            <div class="metric-value" id="cancelRate">-</div>
            <div class="comparisons">
                <div class="comparison">
                    <span class="comparison-label">vs Last Month</span>
                    <span class="comparison-value" id="cancelVsLastMonth">-</span>
                </div>
                <div class="comparison">
                    <span class="comparison-label">vs Last Year</span>
                    <span class="comparison-value" id="cancelVsLastYear">-</span>
                </div>
            </div>
        </div>
    </div>
    
    <div class="status-breakdown">
        <div class="status-grid">
            <div class="status-item">
                <h4>✅ Delivered</h4>
                <span class="pct delivered" id="deliveredPct">-</span>
                <span class="value" id="deliveredCount">-</span>
            </div>
            <div class="status-item">
                <h4>❌ Cancelled</h4>
                <span class="pct cancelled" id="cancelledPct">-</span>
                <span class="value" id="cancelledCount">-</span>
            </div>
            <div class="status-item">
                <h4>↩️ Returned</h4>
                <span class="pct returned" id="returnedPct">-</span>
                <span class="value" id="returnedCount">-</span>
            </div>
            <div class="status-item">
                <h4>⚠️ Failed Delivery</h4>
                <span class="pct failed" id="failedPct">-</span>
                <span class="value" id="failedCount">-</span>
            </div>
        </div>
    </div>
    
    <div class="charts-grid">
        <div class="chart-card">
            <h3>📈 Orders Trend (12 Months)</h3>
            <div id="trendChart"></div>
        </div>
        <div class="chart-card">
            <h3>📊 3-Month Comparison</h3>
            <div id="comparisonChart"></div>
        </div>
    </div>

    <script>
        const monthlyData = MONTHLY_DATA_PLACEHOLDER;
        
        // Sort data by year and month
        monthlyData.sort((a, b) => {
            if (a.Year !== b.Year) return a.Year - b.Year;
            return a.Month - b.Month;
        });
        
        // Populate month selector
        const selector = document.getElementById('monthSelector');
        const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        
        // Add options in reverse order (most recent first)
        [...monthlyData].reverse().forEach((d, i) => {
            const option = document.createElement('option');
            option.value = `${d.Year}-${d.Month}`;
            option.textContent = `${monthNames[d.Month]} ${d.Year}`;
            if (i === 0) option.selected = true;
            selector.appendChild(option);
        });
        
        function getMonthData(year, month) {
            return monthlyData.find(d => d.Year === year && d.Month === month);
        }
        
        function getLastMonth(year, month) {
            if (month === 1) return { year: year - 1, month: 12 };
            return { year, month: month - 1 };
        }
        
        function formatChange(current, previous, suffix = '', isRateCompare = false) {
            if (!previous || previous === 0) return '<span class="neutral">N/A</span>';
            
            const change = isRateCompare 
                ? (current - previous).toFixed(1)
                : (((current - previous) / previous) * 100).toFixed(1);
            
            const arrow = change > 0 ? '↑' : change < 0 ? '↓' : '';
            const cls = change > 0 ? 'positive' : change < 0 ? 'negative' : 'neutral';
            
            // For cancel rate, positive is bad
            const adjustedCls = suffix === '%' && current > previous ? 'negative' : cls;
            
            return `<span class="${isRateCompare ? (change > 0 ? 'negative' : 'positive') : cls}">${arrow} ${Math.abs(change)}${isRateCompare ? 'pp' : '%'}</span>`;
        }
        
        function formatNumber(num) {
            return num.toLocaleString();
        }
        
        function updateDashboard() {
            const [year, month] = selector.value.split('-').map(Number);
            const current = getMonthData(year, month);
            
            if (!current) return;
            
            // Get comparison months
            const lastMonthInfo = getLastMonth(year, month);
            const lastMonth = getMonthData(lastMonthInfo.year, lastMonthInfo.month);
            const lastYear = getMonthData(year - 1, month);
            
            // Update metrics
            document.getElementById('totalOrders').textContent = formatNumber(current.total_orders);
            document.getElementById('deliveryRate').textContent = current.delivery_rate + '%';
            document.getElementById('cancelRate').textContent = current.cancel_rate + '%';
            
            // Update comparisons
            document.getElementById('totalVsLastMonth').innerHTML = 
                formatChange(current.total_orders, lastMonth?.total_orders);
            document.getElementById('totalVsLastYear').innerHTML = 
                formatChange(current.total_orders, lastYear?.total_orders);
            
            document.getElementById('deliveryVsLastMonth').innerHTML = 
                formatChange(current.delivery_rate, lastMonth?.delivery_rate, '%', true);
            document.getElementById('deliveryVsLastYear').innerHTML = 
                formatChange(current.delivery_rate, lastYear?.delivery_rate, '%', true);
            
            // For cancel rate, invert the colors (higher is worse)
            const cancelVsLM = lastMonth ? (current.cancel_rate - lastMonth.cancel_rate).toFixed(1) : null;
            const cancelVsLY = lastYear ? (current.cancel_rate - lastYear.cancel_rate).toFixed(1) : null;
            
            document.getElementById('cancelVsLastMonth').innerHTML = cancelVsLM !== null
                ? `<span class="${cancelVsLM > 0 ? 'negative' : cancelVsLM < 0 ? 'positive' : 'neutral'}">${cancelVsLM > 0 ? '↑' : '↓'} ${Math.abs(cancelVsLM)}pp</span>`
                : '<span class="neutral">N/A</span>';
            document.getElementById('cancelVsLastYear').innerHTML = cancelVsLY !== null
                ? `<span class="${cancelVsLY > 0 ? 'negative' : cancelVsLY < 0 ? 'positive' : 'neutral'}">${cancelVsLY > 0 ? '↑' : '↓'} ${Math.abs(cancelVsLY)}pp</span>`
                : '<span class="neutral">N/A</span>';
            
            // Update status breakdown
            document.getElementById('deliveredPct').textContent = `${current.delivery_rate}%`;
            document.getElementById('deliveredCount').textContent = `(${formatNumber(current.delivered)})`;
            document.getElementById('cancelledPct').textContent = `${current.cancel_rate}%`;
            document.getElementById('cancelledCount').textContent = `(${formatNumber(current.cancelled)})`;
            document.getElementById('returnedPct').textContent = `${(current.returned/current.total_orders*100).toFixed(1)}%`;
            document.getElementById('returnedCount').textContent = `(${formatNumber(current.returned)})`;
            document.getElementById('failedPct').textContent = `${(current.failed/current.total_orders*100).toFixed(1)}%`;
            document.getElementById('failedCount').textContent = `(${formatNumber(current.failed)})`;
            
            // Update charts
            updateTrendChart(year, month);
            updateComparisonChart(year, month, lastMonth, lastYear);
        }
        
        function updateTrendChart(currentYear, currentMonth) {
            // Get last 12 months of data
            const currentIdx = monthlyData.findIndex(d => d.Year === currentYear && d.Month === currentMonth);
            const startIdx = Math.max(0, currentIdx - 11);
            const trendData = monthlyData.slice(startIdx, currentIdx + 1);
            
            const labels = trendData.map(d => `${monthNames[d.Month]} ${d.Year}`);
            
            const trace1 = {
                x: labels,
                y: trendData.map(d => d.total_orders),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Total Orders',
                line: { color: '#00d4aa', width: 3 },
                marker: { size: 8 }
            };
            
            const trace2 = {
                x: labels,
                y: trendData.map(d => d.delivered),
                type: 'scatter',
                mode: 'lines+markers',
                name: 'Delivered',
                line: { color: '#4ecdc4', width: 2 },
                marker: { size: 6 }
            };
            
            const layout = {
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: { color: '#888' },
                margin: { t: 20, r: 20, b: 60, l: 60 },
                xaxis: { 
                    gridcolor: 'rgba(255,255,255,0.1)',
                    tickangle: -45
                },
                yaxis: { gridcolor: 'rgba(255,255,255,0.1)' },
                legend: { orientation: 'h', y: 1.1 },
                showlegend: true
            };
            
            Plotly.newPlot('trendChart', [trace1, trace2], layout, {responsive: true});
        }
        
        function updateComparisonChart(year, month, lastMonth, lastYear) {
            const current = getMonthData(year, month);
            const categories = ['Total', 'Delivered', 'Cancelled', 'Returned'];
            
            const currentValues = [current.total_orders, current.delivered, current.cancelled, current.returned];
            const lastMonthValues = lastMonth 
                ? [lastMonth.total_orders, lastMonth.delivered, lastMonth.cancelled, lastMonth.returned]
                : [0, 0, 0, 0];
            const lastYearValues = lastYear
                ? [lastYear.total_orders, lastYear.delivered, lastYear.cancelled, lastYear.returned]
                : [0, 0, 0, 0];
            
            const trace1 = {
                x: categories,
                y: currentValues,
                type: 'bar',
                name: `${monthNames[month]} ${year}`,
                marker: { color: '#00d4aa' }
            };
            
            const trace2 = {
                x: categories,
                y: lastMonthValues,
                type: 'bar',
                name: lastMonth ? `${monthNames[lastMonth.Month]} ${lastMonth.Year}` : 'Last Month',
                marker: { color: '#4a90a4' }
            };
            
            const trace3 = {
                x: categories,
                y: lastYearValues,
                type: 'bar',
                name: lastYear ? `${monthNames[month]} ${year-1}` : 'Last Year',
                marker: { color: '#8b5cf6' }
            };
            
            const layout = {
                paper_bgcolor: 'rgba(0,0,0,0)',
                plot_bgcolor: 'rgba(0,0,0,0)',
                font: { color: '#888' },
                margin: { t: 20, r: 20, b: 40, l: 60 },
                xaxis: { gridcolor: 'rgba(255,255,255,0.1)' },
                yaxis: { gridcolor: 'rgba(255,255,255,0.1)' },
                barmode: 'group',
                legend: { orientation: 'h', y: 1.15 }
            };
            
            Plotly.newPlot('comparisonChart', [trace1, trace2, trace3], layout, {responsive: true});
        }
        
        // Initial load
        updateDashboard();
    </script>
</body>
</html>
"""
    
    # Replace placeholder with actual data
    import json
    html_content = html_content.replace('MONTHLY_DATA_PLACEHOLDER', json.dumps(data_js))
    
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"\n✅ Dashboard saved to: {output_file}")


if __name__ == "__main__":
    df = load_all_data()
    generate_html_dashboard(df)

