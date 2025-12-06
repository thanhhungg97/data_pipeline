import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom';
import { useEffect, useMemo, useState } from 'react';

import Select from 'react-select';

// Types
interface MonthlyData {
  Year: number;
  Month: number;
  total_orders: number;
  delivered: number;
  cancelled: number;
  returned: number;
  failed: number;
}

interface SourceMetric {
  Source: string;
  Year: number;
  Month: number;
  total_orders: number;
  delivered: number;
  cancelled: number;
  returned: number;
  failed: number;
  delivery_rate: number;
  cancel_rate: number;
}

interface ReasonData {
  Year: number;
  Month: number;
  'Reason cancelled': string;
  count: number;
}

interface DashboardData {
  monthly: MonthlyData[];
  sources: string[];
  metrics: SourceMetric[];
  reasons: ReasonData[];
}

interface SelectOption {
  value: string;
  label: string;
}

// Constants
const MONTH_NAMES = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const COLORS = ['#00d4aa', '#ff6b6b', '#ffa726', '#ab47bc', '#42a5f5', '#26a69a'];
const SOURCE_COLORS = ['#ee4d2d', '#00d4aa', '#ff6b9d', '#ffa726', '#42a5f5', '#ab47bc'];

// Utility functions
const fmt = (n: number) => n?.toLocaleString() ?? '0';
const pct = (n: number) => `${n?.toFixed(1) ?? '0'}%`;
const monthLabel = (y: number, m: number) => `${MONTH_NAMES[m]} ${y}`;

// Tooltip style (lighter dark bg, white text)
const tooltipStyle = {
  contentStyle: { background: '#1e293b', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 10, color: '#fff', boxShadow: '0 10px 40px rgba(0,0,0,0.3)' },
  labelStyle: { color: '#fff', fontWeight: 600, marginBottom: 4 },
  itemStyle: { color: '#f1f5f9' },
};

// Main App
function App() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Check for embedded data first (for file:// protocol, no CORS)
    const embeddedData = (window as unknown as { __DASHBOARD_DATA__?: DashboardData }).__DASHBOARD_DATA__;
    if (embeddedData) {
      setData(embeddedData);
      setLoading(false);
      return;
    }

    // Only fetch if we're on http/https (not file://)
    if (window.location.protocol === 'file:') {
      setError('No embedded data found. Please regenerate the dashboard.');
      setLoading(false);
      return;
    }

    // Fallback to fetch for dev server
    fetch('./data.json')
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(json => { setData(json); setLoading(false); })
      .catch(err => { setError(err.message); setLoading(false); });
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-white text-xl animate-pulse">Loading dashboard...</div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-xl mb-4">Failed to load data</div>
          <p className="text-gray-400">{error || 'No data available'}</p>
        </div>
      </div>
    );
  }

  return (
    <BrowserRouter>
      <div className="min-h-screen">
        <Navigation />
        <Routes>
          <Route path="/" element={<Overview data={data} />} />
          <Route path="/compare" element={<Compare data={data} />} />
          <Route path="/sources" element={<Sources data={data} />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

// Navigation
function Navigation() {
  const links = [
    { to: '/', label: '📊 Overview' },
    { to: '/compare', label: '📈 Monthly Compare' },
    { to: '/sources', label: '🏪 Sources' },
  ];

  return (
    <nav className="bg-slate-800/80 backdrop-blur-md border-b border-slate-600/50 px-6 py-3 sticky top-0 z-40">
      <div className="flex gap-2">
        {links.map(({ to, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                isActive
                  ? 'bg-emerald-500 text-white shadow-lg shadow-emerald-500/30'
                  : 'text-slate-300 hover:text-white hover:bg-slate-700/50'
              }`
            }
          >
            {label}
          </NavLink>
        ))}
      </div>
    </nav>
  );
}

// Card Component
function Card({ children, title, className = '' }: { children: React.ReactNode; title?: string; className?: string }) {
  return (
    <div className={`bg-slate-800/60 backdrop-blur-sm border border-slate-600/50 rounded-xl p-5 card-hover ${className}`}>
      {title && <h3 className="text-white font-semibold mb-4 text-lg">{title}</h3>}
      {children}
    </div>
  );
}

// Stat Card
function StatCard({ label, value, color = 'white', sub }: { label: string; value: string; color?: string; sub?: React.ReactNode }) {
  const colorClass = {
    white: 'text-white',
    green: 'text-emerald-400',
    red: 'text-red-400',
    orange: 'text-orange-400',
    purple: 'text-purple-400',
  }[color] || 'text-white';

  return (
    <Card>
      <div className="text-gray-400 text-xs uppercase tracking-wider mb-2">{label}</div>
      <div className={`text-3xl font-bold ${colorClass}`}>{value}</div>
      {sub && <div className="mt-3">{sub}</div>}
    </Card>
  );
}

// Year-Month Picker Component
function YearMonthPicker({ 
  label, 
  year, 
  month, 
  years, 
  monthsInYear, 
  onYearChange, 
  onMonthChange 
}: {
  label: string;
  year: number;
  month: number;
  years: number[];
  monthsInYear: number[];
  onYearChange: (y: number) => void;
  onMonthChange: (m: number) => void;
}) {
  return (
    <div>
      <label className="block text-slate-400 text-xs uppercase tracking-wider mb-2">{label}</label>
      <div className="flex gap-2">
        <select
          value={year}
          onChange={(e) => onYearChange(Number(e.target.value))}
          className="bg-slate-700/80 text-white border border-slate-500/50 rounded-lg px-3 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:border-transparent cursor-pointer hover:bg-slate-600/80 transition"
        >
          {years.map(y => <option key={y} value={y}>{y}</option>)}
        </select>
        <div className="grid grid-cols-4 gap-1 bg-slate-700/60 p-1.5 rounded-lg border border-slate-500/50">
          {[1,2,3,4,5,6,7,8,9,10,11,12].map(m => {
            const available = monthsInYear.includes(m);
            const selected = month === m;
            return (
              <button
                key={m}
                disabled={!available}
                onClick={() => available && onMonthChange(m)}
                className={`w-10 h-8 text-xs font-medium rounded transition-all ${
                  selected 
                    ? 'bg-emerald-500 text-white shadow-lg' 
                    : available 
                      ? 'text-slate-300 hover:bg-slate-600 hover:text-white' 
                      : 'text-slate-600 cursor-not-allowed'
                }`}
              >
                {MONTH_NAMES[m]}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// Overview Page
function Overview({ data }: { data: DashboardData }) {
  // Get unique years and months per year
  const yearMonthMap = useMemo(() => {
    const map: Record<number, number[]> = {};
    data.monthly.forEach(d => {
      if (!map[d.Year]) map[d.Year] = [];
      if (!map[d.Year].includes(d.Month)) map[d.Year].push(d.Month);
    });
    Object.keys(map).forEach(y => map[Number(y)].sort((a, b) => a - b));
    return map;
  }, [data.monthly]);

  const years = useMemo(() => Object.keys(yearMonthMap).map(Number).sort((a, b) => a - b), [yearMonthMap]);
  
  // Start selection
  const [startYear, setStartYear] = useState(years[0]);
  const [startMonth, setStartMonth] = useState(yearMonthMap[years[0]]?.[0] || 1);
  
  // End selection  
  const [endYear, setEndYear] = useState(years[years.length - 1]);
  const [endMonth, setEndMonth] = useState(yearMonthMap[years[years.length - 1]]?.slice(-1)[0] || 12);

  // Find indices
  const startIdx = useMemo(() => 
    data.monthly.findIndex(d => d.Year === startYear && d.Month === startMonth),
    [data.monthly, startYear, startMonth]
  );
  const endIdx = useMemo(() => 
    data.monthly.findIndex(d => d.Year === endYear && d.Month === endMonth),
    [data.monthly, endYear, endMonth]
  );

  const filtered = useMemo(() => {
    const start = Math.max(0, startIdx);
    const end = Math.max(start, endIdx === -1 ? data.monthly.length - 1 : endIdx);
    return data.monthly.slice(start, end + 1);
  }, [data.monthly, startIdx, endIdx]);

  const summary = useMemo(() => {
    const total = filtered.reduce((s, d) => s + d.total_orders, 0);
    const delivered = filtered.reduce((s, d) => s + d.delivered, 0);
    const cancelled = filtered.reduce((s, d) => s + d.cancelled, 0);
    const returned = filtered.reduce((s, d) => s + d.returned, 0);
    const failed = filtered.reduce((s, d) => s + d.failed, 0);
    return {
      total, delivered, cancelled, returned, failed,
      deliveryRate: total > 0 ? (delivered / total) * 100 : 0,
      cancelRate: total > 0 ? (cancelled / total) * 100 : 0,
      returnRate: total > 0 ? (returned / total) * 100 : 0,
      failedRate: total > 0 ? (failed / total) * 100 : 0,
    };
  }, [filtered]);

  const chartData = filtered.map(d => ({ name: monthLabel(d.Year, d.Month), orders: d.total_orders }));
  const pieData = [
    { name: 'Delivered', value: summary.delivered },
    { name: 'Cancelled', value: summary.cancelled },
    { name: 'Returned', value: summary.returned },
    { name: 'Failed', value: summary.failed },
  ];

  const reasonsAgg = useMemo(() => {
    const startKey = startYear * 100 + startMonth;
    const endKey = endYear * 100 + endMonth;
    const filteredReasons = data.reasons.filter(r => {
      const key = r.Year * 100 + r.Month;
      return key >= startKey && key <= endKey;
    });
    const agg: Record<string, number> = {};
    filteredReasons.forEach(r => { agg[r['Reason cancelled']] = (agg[r['Reason cancelled']] || 0) + r.count; });
    return Object.entries(agg).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value).slice(0, 8);
  }, [data.reasons, startYear, startMonth, endYear, endMonth]);

  const handleReset = () => {
    setStartYear(years[0]);
    setStartMonth(yearMonthMap[years[0]]?.[0] || 1);
    setEndYear(years[years.length - 1]);
    setEndMonth(yearMonthMap[years[years.length - 1]]?.slice(-1)[0] || 12);
  };

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">📊 Overview Dashboard</h1>
        <p className="text-slate-400 mt-1">Performance summary across all data</p>
      </div>

      {/* Date Range */}
      <div className="flex flex-wrap gap-6 items-start">
        <YearMonthPicker
          label="From"
          year={startYear}
          month={startMonth}
          years={years}
          monthsInYear={yearMonthMap[startYear] || []}
          onYearChange={(y) => { setStartYear(y); setStartMonth(yearMonthMap[y]?.[0] || 1); }}
          onMonthChange={setStartMonth}
        />
        <YearMonthPicker
          label="To"
          year={endYear}
          month={endMonth}
          years={years}
          monthsInYear={yearMonthMap[endYear] || []}
          onYearChange={(y) => { setEndYear(y); setEndMonth(yearMonthMap[y]?.slice(-1)[0] || 12); }}
          onMonthChange={setEndMonth}
        />
        <div className="pt-6">
          <button
            onClick={handleReset}
            className="px-4 py-2.5 bg-slate-600/50 hover:bg-slate-500/50 text-white rounded-lg transition border border-slate-500/50"
          >
            Reset All
          </button>
        </div>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Total Orders" value={fmt(summary.total)} />
        <StatCard label="Delivery Rate" value={pct(summary.deliveryRate)} color="green" />
        <StatCard label="Cancel Rate" value={pct(summary.cancelRate)} color="red" />
        <StatCard label="Return Rate" value={pct(summary.returnRate)} color="orange" />
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { icon: '✅', label: 'Delivered', value: summary.delivered, rate: summary.deliveryRate, color: 'text-emerald-400' },
          { icon: '❌', label: 'Cancelled', value: summary.cancelled, rate: summary.cancelRate, color: 'text-red-400' },
          { icon: '↩️', label: 'Returned', value: summary.returned, rate: summary.returnRate, color: 'text-orange-400' },
          { icon: '⚠️', label: 'Failed', value: summary.failed, rate: summary.failedRate, color: 'text-purple-400' },
        ].map(({ icon, label, value, rate, color }) => (
          <Card key={label}>
            <div className="text-gray-400 text-sm mb-2">{icon} {label}</div>
            <span className={`text-2xl font-bold ${color}`}>{pct(rate)}</span>
            <span className="text-gray-500 ml-2">({fmt(value)})</span>
          </Card>
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card title="📈 Monthly Order Volume" className="lg:col-span-2">
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="colorOrders" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#00d4aa" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#00d4aa" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
              <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
              <YAxis 
                stroke="#888" 
                tick={{ fill: '#888', fontSize: 11 }} 
                label={{ value: 'Total Orders', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 12 }}
              />
              <Tooltip {...tooltipStyle} formatter={(value: number) => [fmt(value) + ' orders', 'Total']} />
              <Area type="monotone" dataKey="orders" name="Total Orders" stroke="#00d4aa" fill="url(#colorOrders)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </Card>
        <Card title="📊 Order Status Distribution">
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={pieData} cx="50%" cy="50%" innerRadius={50} outerRadius={90} dataKey="value" label={({ percent }) => `${((percent ?? 0) * 100).toFixed(0)}%`}>
                {pieData.map((_, i) => <Cell key={i} fill={COLORS[i]} />)}
              </Pie>
              <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </Card>
      </div>

      {/* Cancellation Reasons */}
      <Card title="❌ Top Cancellation Reasons">
        {reasonsAgg.length > 0 ? (
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={reasonsAgg.map(r => ({ ...r, shortName: r.name.length > 25 ? r.name.slice(0, 22) + '...' : r.name }))} layout="vertical" margin={{ left: 20, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
              <XAxis type="number" stroke="#94a3b8" tick={{ fill: '#94a3b8' }} label={{ value: 'Number of Orders', position: 'bottom', fill: '#94a3b8', fontSize: 12 }} />
              <YAxis type="category" dataKey="shortName" stroke="#94a3b8" tick={{ fill: '#e2e8f0', fontSize: 12 }} width={160} />
              <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
              <Bar dataKey="value" fill="#f87171" radius={[0, 6, 6, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-gray-500 text-center py-10">No data for selected range</p>
        )}
      </Card>
    </div>
  );
}

// Compare Page
function Compare({ data }: { data: DashboardData }) {
  const sourceOpts: SelectOption[] = [{ value: 'All', label: '📊 All Sources' }, ...data.sources.map(s => ({ value: s, label: s }))];
  const [source, setSource] = useState<SelectOption>(sourceOpts[0]);

  const filtered = useMemo(() => 
    source.value === 'All' ? data.metrics.filter(m => m.Source === 'All') : data.metrics.filter(m => m.Source === source.value),
    [data.metrics, source]
  );

  // Build year-month map from filtered data
  const yearMonthMap = useMemo(() => {
    const map: Record<number, number[]> = {};
    filtered.forEach(d => {
      if (!map[d.Year]) map[d.Year] = [];
      if (!map[d.Year].includes(d.Month)) map[d.Year].push(d.Month);
    });
    Object.keys(map).forEach(y => map[Number(y)].sort((a, b) => a - b));
    return map;
  }, [filtered]);

  const years = useMemo(() => Object.keys(yearMonthMap).map(Number).sort((a, b) => b - a), [yearMonthMap]);
  
  const [selectedYear, setSelectedYear] = useState(years[0] || 2025);
  const [selectedMonth, setSelectedMonth] = useState(yearMonthMap[years[0]]?.slice(-1)[0] || 1);

  // Update selection when source changes
  useMemo(() => {
    if (years.length > 0 && !years.includes(selectedYear)) {
      setSelectedYear(years[0]);
      setSelectedMonth(yearMonthMap[years[0]]?.slice(-1)[0] || 1);
    }
  }, [years, selectedYear, yearMonthMap]);

  const current = useMemo(() => 
    filtered.find(d => d.Year === selectedYear && d.Month === selectedMonth) || null,
    [filtered, selectedYear, selectedMonth]
  );

  // Last month data
  const lastMonth = useMemo(() => {
    const prevM = selectedMonth === 1 ? 12 : selectedMonth - 1;
    const prevY = selectedMonth === 1 ? selectedYear - 1 : selectedYear;
    return filtered.find(d => d.Year === prevY && d.Month === prevM) || null;
  }, [filtered, selectedYear, selectedMonth]);

  // Same month last year
  const lastYearData = useMemo(() => 
    filtered.find(d => d.Year === selectedYear - 1 && d.Month === selectedMonth) || null,
    [filtered, selectedYear, selectedMonth]
  );

  // Comparison chart data
  const comparisonData = useMemo(() => {
    if (!current) return [];
    return [
      { name: 'Total', current: current.total_orders, lastMonth: lastMonth?.total_orders || 0, lastYear: lastYearData?.total_orders || 0 },
      { name: 'Delivered', current: current.delivered, lastMonth: lastMonth?.delivered || 0, lastYear: lastYearData?.delivered || 0 },
      { name: 'Cancelled', current: current.cancelled, lastMonth: lastMonth?.cancelled || 0, lastYear: lastYearData?.cancelled || 0 },
      { name: 'Returned', current: current.returned, lastMonth: lastMonth?.returned || 0, lastYear: lastYearData?.returned || 0 },
    ];
  }, [current, lastMonth, lastYearData]);

  // Calculate changes
  const momChange = current && lastMonth ? ((current.total_orders - lastMonth.total_orders) / lastMonth.total_orders * 100) : null;
  const yoyChange = current && lastYearData ? ((current.total_orders - lastYearData.total_orders) / lastYearData.total_orders * 100) : null;
  const momDelivery = current && lastMonth ? (current.delivery_rate - lastMonth.delivery_rate) * 100 : null;
  const yoyDelivery = current && lastYearData ? (current.delivery_rate - lastYearData.delivery_rate) * 100 : null;

  const sorted = useMemo(() => [...filtered].sort((a, b) => a.Year !== b.Year ? a.Year - b.Year : a.Month - b.Month), [filtered]);
  const chartData = sorted.slice(-12).map(d => ({ name: monthLabel(d.Year, d.Month), total: d.total_orders, delivered: d.delivered }));

  if (!current) return <div className="p-6 text-white">No data for selected period</div>;

  const y = selectedYear;
  const m = selectedMonth;
  const prevM = m === 1 ? 12 : m - 1;
  const prevY = m === 1 ? y - 1 : y;

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">📈 Monthly Comparison</h1>
        <p className="text-slate-400 mt-1">Compare month-over-month and year-over-year performance</p>
      </div>

      <div className="flex flex-wrap gap-6 items-start">
        <div>
          <label className="block text-slate-400 text-xs uppercase tracking-wider mb-2">Source</label>
          <Select value={source} onChange={(v) => v && setSource(v)} options={sourceOpts} className="w-52" classNamePrefix="react-select" />
        </div>
        <YearMonthPicker
          label="Select Month"
          year={selectedYear}
          month={selectedMonth}
          years={years}
          monthsInYear={yearMonthMap[selectedYear] || []}
          onYearChange={(y) => { setSelectedYear(y); setSelectedMonth(yearMonthMap[y]?.slice(-1)[0] || 1); }}
          onMonthChange={setSelectedMonth}
        />
      </div>

      {/* Current Month Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Total Orders" value={fmt(current.total_orders)} />
        <StatCard label="Delivery Rate" value={pct(current.delivery_rate)} color="green" />
        <StatCard label="Cancel Rate" value={pct(current.cancel_rate)} color="red" />
        <StatCard label="Delivered" value={fmt(current.delivered)} color="green" />
      </div>

      {/* MoM & YoY Changes */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <div className="text-gray-400 text-xs uppercase mb-1">MoM Orders</div>
          <div className={`text-xl font-bold ${momChange && momChange >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {momChange !== null ? `${momChange >= 0 ? '▲' : '▼'} ${Math.abs(momChange).toFixed(1)}%` : 'N/A'}
          </div>
          <div className="text-gray-500 text-xs">vs {monthLabel(prevY, prevM)}</div>
        </Card>
        <Card>
          <div className="text-gray-400 text-xs uppercase mb-1">YoY Orders</div>
          <div className={`text-xl font-bold ${yoyChange && yoyChange >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {yoyChange !== null ? `${yoyChange >= 0 ? '▲' : '▼'} ${Math.abs(yoyChange).toFixed(1)}%` : 'N/A'}
          </div>
          <div className="text-gray-500 text-xs">vs {monthLabel(y - 1, m)}</div>
        </Card>
        <Card>
          <div className="text-gray-400 text-xs uppercase mb-1">MoM Delivery</div>
          <div className={`text-xl font-bold ${momDelivery && momDelivery >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {momDelivery !== null ? `${momDelivery >= 0 ? '▲' : '▼'} ${Math.abs(momDelivery).toFixed(1)}pp` : 'N/A'}
          </div>
          <div className="text-gray-500 text-xs">vs {monthLabel(prevY, prevM)}</div>
        </Card>
        <Card>
          <div className="text-gray-400 text-xs uppercase mb-1">YoY Delivery</div>
          <div className={`text-xl font-bold ${yoyDelivery && yoyDelivery >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {yoyDelivery !== null ? `${yoyDelivery >= 0 ? '▲' : '▼'} ${Math.abs(yoyDelivery).toFixed(1)}pp` : 'N/A'}
          </div>
          <div className="text-gray-500 text-xs">vs {monthLabel(y - 1, m)}</div>
        </Card>
      </div>

      {/* 3-Month Comparison Chart */}
      <Card title="📊 Order Count Comparison (Current vs Previous Months)">
        <div className="flex gap-4 mb-4 text-sm">
          <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-[#00d4aa]"></span> {monthLabel(y, m)} (Current)</div>
          {lastMonth && <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-[#ff6b6b]"></span> {monthLabel(prevY, prevM)} (Last Month)</div>}
          {lastYearData && <div className="flex items-center gap-2"><span className="w-3 h-3 rounded bg-[#ffd93d]"></span> {monthLabel(y - 1, m)} (Last Year)</div>}
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={comparisonData} barGap={4}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
            <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888' }} />
            <YAxis 
              stroke="#888" 
              tick={{ fill: '#888' }} 
              label={{ value: 'Number of Orders', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 12 }}
            />
            <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
            <Bar dataKey="current" name={monthLabel(y, m)} fill="#00d4aa" radius={[4, 4, 0, 0]} />
            {lastMonth && <Bar dataKey="lastMonth" name={monthLabel(prevY, prevM)} fill="#ff6b6b" radius={[4, 4, 0, 0]} />}
            {lastYearData && <Bar dataKey="lastYear" name={monthLabel(y - 1, m)} fill="#ffd93d" radius={[4, 4, 0, 0]} />}
          </BarChart>
        </ResponsiveContainer>
      </Card>

      {/* Detailed Comparison Table */}
      <Card title="📋 Detailed Comparison">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-400 border-b border-white/10">
                <th className="text-left py-3 px-2">Metric</th>
                <th className="text-right py-3 px-2">{monthLabel(y, m)}</th>
                <th className="text-right py-3 px-2">{monthLabel(prevY, prevM)}</th>
                <th className="text-right py-3 px-2">MoM</th>
                <th className="text-right py-3 px-2">{monthLabel(y - 1, m)}</th>
                <th className="text-right py-3 px-2">YoY</th>
              </tr>
            </thead>
            <tbody className="text-white">
              <tr className="border-b border-white/5">
                <td className="py-3 px-2">Total Orders</td>
                <td className="text-right py-3 px-2 font-semibold">{fmt(current.total_orders)}</td>
                <td className="text-right py-3 px-2 text-gray-400">{lastMonth ? fmt(lastMonth.total_orders) : '-'}</td>
                <td className={`text-right py-3 px-2 ${momChange && momChange >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {momChange !== null ? `${momChange >= 0 ? '+' : ''}${momChange.toFixed(1)}%` : '-'}
                </td>
                <td className="text-right py-3 px-2 text-gray-400">{lastYearData ? fmt(lastYearData.total_orders) : '-'}</td>
                <td className={`text-right py-3 px-2 ${yoyChange && yoyChange >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {yoyChange !== null ? `${yoyChange >= 0 ? '+' : ''}${yoyChange.toFixed(1)}%` : '-'}
                </td>
              </tr>
              <tr className="border-b border-white/5">
                <td className="py-3 px-2">Delivered</td>
                <td className="text-right py-3 px-2 font-semibold text-emerald-400">{fmt(current.delivered)}</td>
                <td className="text-right py-3 px-2 text-gray-400">{lastMonth ? fmt(lastMonth.delivered) : '-'}</td>
                <td className="text-right py-3 px-2 text-gray-400">
                  {lastMonth ? `${((current.delivered - lastMonth.delivered) / lastMonth.delivered * 100).toFixed(1)}%` : '-'}
                </td>
                <td className="text-right py-3 px-2 text-gray-400">{lastYearData ? fmt(lastYearData.delivered) : '-'}</td>
                <td className="text-right py-3 px-2 text-gray-400">
                  {lastYearData ? `${((current.delivered - lastYearData.delivered) / lastYearData.delivered * 100).toFixed(1)}%` : '-'}
                </td>
              </tr>
              <tr className="border-b border-white/5">
                <td className="py-3 px-2">Cancelled</td>
                <td className="text-right py-3 px-2 font-semibold text-red-400">{fmt(current.cancelled)}</td>
                <td className="text-right py-3 px-2 text-gray-400">{lastMonth ? fmt(lastMonth.cancelled) : '-'}</td>
                <td className="text-right py-3 px-2 text-gray-400">
                  {lastMonth && lastMonth.cancelled > 0 ? `${((current.cancelled - lastMonth.cancelled) / lastMonth.cancelled * 100).toFixed(1)}%` : '-'}
                </td>
                <td className="text-right py-3 px-2 text-gray-400">{lastYearData ? fmt(lastYearData.cancelled) : '-'}</td>
                <td className="text-right py-3 px-2 text-gray-400">
                  {lastYearData && lastYearData.cancelled > 0 ? `${((current.cancelled - lastYearData.cancelled) / lastYearData.cancelled * 100).toFixed(1)}%` : '-'}
                </td>
              </tr>
              <tr className="border-b border-white/5">
                <td className="py-3 px-2">Delivery Rate</td>
                <td className="text-right py-3 px-2 font-semibold">{pct(current.delivery_rate)}</td>
                <td className="text-right py-3 px-2 text-gray-400">{lastMonth ? pct(lastMonth.delivery_rate) : '-'}</td>
                <td className={`text-right py-3 px-2 ${momDelivery && momDelivery >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {momDelivery !== null ? `${momDelivery >= 0 ? '+' : ''}${momDelivery.toFixed(1)}pp` : '-'}
                </td>
                <td className="text-right py-3 px-2 text-gray-400">{lastYearData ? pct(lastYearData.delivery_rate) : '-'}</td>
                <td className={`text-right py-3 px-2 ${yoyDelivery && yoyDelivery >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {yoyDelivery !== null ? `${yoyDelivery >= 0 ? '+' : ''}${yoyDelivery.toFixed(1)}pp` : '-'}
                </td>
              </tr>
              <tr>
                <td className="py-3 px-2">Cancel Rate</td>
                <td className="text-right py-3 px-2 font-semibold">{pct(current.cancel_rate)}</td>
                <td className="text-right py-3 px-2 text-gray-400">{lastMonth ? pct(lastMonth.cancel_rate) : '-'}</td>
                <td className={`text-right py-3 px-2 ${lastMonth && current.cancel_rate <= lastMonth.cancel_rate ? 'text-emerald-400' : 'text-red-400'}`}>
                  {lastMonth ? `${(current.cancel_rate - lastMonth.cancel_rate) * 100 >= 0 ? '+' : ''}${((current.cancel_rate - lastMonth.cancel_rate) * 100).toFixed(1)}pp` : '-'}
                </td>
                <td className="text-right py-3 px-2 text-gray-400">{lastYearData ? pct(lastYearData.cancel_rate) : '-'}</td>
                <td className={`text-right py-3 px-2 ${lastYearData && current.cancel_rate <= lastYearData.cancel_rate ? 'text-emerald-400' : 'text-red-400'}`}>
                  {lastYearData ? `${(current.cancel_rate - lastYearData.cancel_rate) * 100 >= 0 ? '+' : ''}${((current.cancel_rate - lastYearData.cancel_rate) * 100).toFixed(1)}pp` : '-'}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="📈 12-Month Order Volume Trend">
        <ResponsiveContainer width="100%" height={350}>
          <AreaChart data={chartData}>
            <defs>
              <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#00d4aa" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="#00d4aa" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
            <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
            <YAxis 
              stroke="#888" 
              tick={{ fill: '#888' }} 
              label={{ value: 'Number of Orders', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 12 }}
            />
            <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
            <Legend />
            <Area type="monotone" dataKey="total" name="Total Orders" stroke="#00d4aa" fill="url(#colorTotal)" strokeWidth={2} />
            <Area type="monotone" dataKey="delivered" name="Delivered Orders" stroke="#4ecdc4" fill="transparent" strokeWidth={2} strokeDasharray="5 5" />
          </AreaChart>
        </ResponsiveContainer>
      </Card>
    </div>
  );
}

// Sources Page
function Sources({ data }: { data: DashboardData }) {
  // Build year-month map
  const yearMonthMap = useMemo(() => {
    const map: Record<number, number[]> = {};
    data.metrics.forEach(d => {
      if (!map[d.Year]) map[d.Year] = [];
      if (!map[d.Year].includes(d.Month)) map[d.Year].push(d.Month);
    });
    Object.keys(map).forEach(y => map[Number(y)].sort((a, b) => a - b));
    return map;
  }, [data.metrics]);

  const years = useMemo(() => Object.keys(yearMonthMap).map(Number).sort((a, b) => b - a), [yearMonthMap]);
  const periods = [...new Set(data.metrics.map(d => `${d.Year}-${d.Month}`))].sort().reverse();
  
  const [selectedYear, setSelectedYear] = useState(years[0] || 2025);
  const [selectedMonth, setSelectedMonth] = useState(yearMonthMap[years[0]]?.slice(-1)[0] || 1);
  const [selected, setSelected] = useState<SelectOption[]>(data.sources.map((s, i) => ({ value: s, label: s, color: SOURCE_COLORS[i] })));

  const sourceColors: Record<string, string> = {};
  data.sources.forEach((s, i) => { sourceColors[s] = SOURCE_COLORS[i % SOURCE_COLORS.length]; });

  const year = selectedYear;
  const mon = selectedMonth;
  const getSource = (s: string) => data.metrics.find(m => m.Source === s && m.Year === year && m.Month === mon);

  const selectedValues = selected.map(s => s.value);

  const chartData = useMemo(() => {
    const last12 = periods.slice(0, 12).reverse();
    return last12.map(p => {
      const [y, m] = p.split('-').map(Number);
      const point: Record<string, string | number> = { name: monthLabel(y, m) };
      selectedValues.forEach(s => {
        const d = data.metrics.find(x => x.Source === s && x.Year === y && x.Month === m);
        point[s] = d?.total_orders || 0;
      });
      return point;
    });
  }, [periods, selectedValues, data.metrics]);

  const barData = ['Total', 'Delivered', 'Cancelled', 'Returned'].map(metric => {
    const point: Record<string, string | number> = { name: metric };
    selectedValues.forEach(s => {
      const d = getSource(s);
      if (d) {
        if (metric === 'Total') point[s] = d.total_orders;
        else if (metric === 'Delivered') point[s] = d.delivered;
        else if (metric === 'Cancelled') point[s] = d.cancelled;
        else point[s] = d.returned;
      }
    });
    return point;
  });

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">🏪 Source Comparison</h1>
        <p className="text-slate-400 mt-1">Compare across sales channels</p>
      </div>

      <div className="flex flex-wrap gap-6 items-start">
        <YearMonthPicker
          label="Select Month"
          year={selectedYear}
          month={selectedMonth}
          years={years}
          monthsInYear={yearMonthMap[selectedYear] || []}
          onYearChange={(y) => { setSelectedYear(y); setSelectedMonth(yearMonthMap[y]?.slice(-1)[0] || 1); }}
          onMonthChange={setSelectedMonth}
        />
        <div className="flex-1 min-w-64">
          <label className="block text-slate-400 text-xs uppercase tracking-wider mb-2">Sources</label>
          <Select
            isMulti
            value={selected}
            onChange={(v) => setSelected(v.length > 0 ? [...v] : [{ value: data.sources[0], label: data.sources[0] }])}
            options={data.sources.map((s, i) => ({ value: s, label: s, color: SOURCE_COLORS[i] }))}
            className="w-full"
            classNamePrefix="react-select"
            closeMenuOnSelect={false}
          />
        </div>
      </div>

      {/* Source Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {selectedValues.map(source => {
          const d = getSource(source);
          const color = sourceColors[source];
          return (
            <Card key={source}>
              <div className="flex items-center gap-2 mb-4">
                <span className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
                <h3 className="font-semibold text-white">{source}</h3>
              </div>
              {d ? (
                <div className="space-y-2">
                  <div className="flex justify-between"><span className="text-gray-400">Orders</span><span className="text-white font-semibold">{fmt(d.total_orders)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-400">Delivery</span><span className="text-emerald-400 font-semibold">{pct(d.delivery_rate)}</span></div>
                  <div className="flex justify-between"><span className="text-gray-400">Cancel</span><span className="text-red-400 font-semibold">{pct(d.cancel_rate)}</span></div>
                </div>
              ) : <p className="text-gray-500">No data</p>}
            </Card>
          );
        })}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card title="📈 Total Orders by Source (12-Month Trend)">
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
              <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
              <YAxis 
                stroke="#888" 
                tick={{ fill: '#888' }} 
                label={{ value: 'Number of Orders', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 12 }} 
              />
              <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
              <Legend />
              {selectedValues.map(s => (
                <Area key={s} type="monotone" dataKey={s} stroke={sourceColors[s]} fill="transparent" strokeWidth={2} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </Card>
        <Card title={`📊 Order Status by Source (${monthLabel(year, mon)})`}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={barData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.15)" fill="transparent" />
              <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888' }} />
              <YAxis 
                stroke="#888" 
                tick={{ fill: '#888' }} 
                label={{ value: 'Number of Orders', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 12 }} 
              />
              <Tooltip {...tooltipStyle} formatter={(value: number, name: string) => [fmt(value) + ' orders', name]} />
              <Legend />
              {selectedValues.map(s => (
                <Bar key={s} dataKey={s} fill={sourceColors[s]} radius={[4, 4, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    </div>
  );
}

export default App;
