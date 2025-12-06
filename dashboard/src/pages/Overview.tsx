import { useState, useMemo } from 'react';
import Select from 'react-select';
import { Card, StatCard, StatusCard } from '../components/Card';
import { TrendChart, StatusPieChart, HorizontalBarChart } from '../components/Charts';
import { formatNumber, formatPercent, formatMonthYear, MONTH_NAMES } from '../utils/format';
import { MonthlyData, ReasonData, SelectOption } from '../types';

interface OverviewProps {
  monthlyData: MonthlyData[];
  reasonsData: ReasonData[];
}

export function Overview({ monthlyData, reasonsData }: OverviewProps) {
  const months = useMemo(() => 
    monthlyData.map((d, i) => ({
      value: i.toString(),
      label: formatMonthYear(d.Year, d.Month),
    })),
    [monthlyData]
  );

  const [startMonth, setStartMonth] = useState<SelectOption>(months[0]);
  const [endMonth, setEndMonth] = useState<SelectOption>(months[months.length - 1]);

  const filteredData = useMemo(() => {
    const startIdx = parseInt(startMonth.value);
    const endIdx = parseInt(endMonth.value);
    return monthlyData.slice(startIdx, endIdx + 1);
  }, [monthlyData, startMonth, endMonth]);

  const summary = useMemo(() => {
    const total = filteredData.reduce((sum, d) => sum + d.total_orders, 0);
    const delivered = filteredData.reduce((sum, d) => sum + d.delivered, 0);
    const cancelled = filteredData.reduce((sum, d) => sum + d.cancelled, 0);
    const returned = filteredData.reduce((sum, d) => sum + d.returned, 0);
    const failed = filteredData.reduce((sum, d) => sum + d.failed, 0);

    return {
      total,
      delivered,
      cancelled,
      returned,
      failed,
      deliveryRate: total > 0 ? (delivered / total) * 100 : 0,
      cancelRate: total > 0 ? (cancelled / total) * 100 : 0,
      returnRate: total > 0 ? (returned / total) * 100 : 0,
      failedRate: total > 0 ? (failed / total) * 100 : 0,
    };
  }, [filteredData]);

  const trendData = useMemo(() =>
    filteredData.map((d) => ({
      name: `${MONTH_NAMES[d.Month]} ${d.Year}`,
      value: d.total_orders,
    })),
    [filteredData]
  );

  const pieData = useMemo(() => [
    { name: 'Delivered', value: summary.delivered },
    { name: 'Cancelled', value: summary.cancelled },
    { name: 'Returned', value: summary.returned },
    { name: 'Failed', value: summary.failed },
  ], [summary]);

  const filteredReasons = useMemo(() => {
    const startIdx = parseInt(startMonth.value);
    const endIdx = parseInt(endMonth.value);
    const startM = monthlyData[startIdx];
    const endM = monthlyData[endIdx];
    
    return reasonsData.filter((r) => {
      const key = r.Year * 100 + r.Month;
      return key >= startM.Year * 100 + startM.Month && key <= endM.Year * 100 + endM.Month;
    });
  }, [reasonsData, monthlyData, startMonth, endMonth]);

  const aggregatedReasons = useMemo(() => {
    const agg: Record<string, number> = {};
    filteredReasons.forEach((r) => {
      const reason = r['Reason cancelled'];
      agg[reason] = (agg[reason] || 0) + r.count;
    });
    return Object.entries(agg)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 10);
  }, [filteredReasons]);

  const handleReset = () => {
    setStartMonth(months[0]);
    setEndMonth(months[months.length - 1]);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* Header */}
      <div style={{ marginBottom: '24px' }}>
        <h1 style={{ fontSize: '28px', fontWeight: 700, color: 'white', margin: 0 }}>📊 Orders Dashboard - Overview</h1>
        <p style={{ color: '#888', marginTop: '4px' }}>Overall performance summary across all data</p>
      </div>

      {/* Date Range Controls */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '16px', alignItems: 'flex-end', marginBottom: '24px' }}>
        <div>
          <label style={{ display: 'block', color: '#888', fontSize: '12px', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '8px' }}>From</label>
          <Select
            value={startMonth}
            onChange={(v) => v && setStartMonth(v)}
            options={months}
            classNamePrefix="react-select"
            styles={{ container: (base) => ({ ...base, width: '180px' }) }}
          />
        </div>
        <div>
          <label style={{ display: 'block', color: '#888', fontSize: '12px', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '8px' }}>To</label>
          <Select
            value={endMonth}
            onChange={(v) => v && setEndMonth(v)}
            options={months}
            classNamePrefix="react-select"
            styles={{ container: (base) => ({ ...base, width: '180px' }) }}
          />
        </div>
        <button
          onClick={handleReset}
          style={{
            padding: '10px 20px',
            background: 'rgba(255,255,255,0.1)',
            border: '1px solid rgba(255,255,255,0.2)',
            borderRadius: '8px',
            color: 'white',
            cursor: 'pointer',
            fontSize: '14px',
          }}
        >
          Reset to All
        </button>
      </div>

      {/* Summary Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px', marginBottom: '24px' }}>
        <StatCard label="Total Orders" value={formatNumber(summary.total)} />
        <StatCard label="Delivery Rate" value={formatPercent(summary.deliveryRate)} color="success" />
        <StatCard label="Date Range" value={`${startMonth.label} → ${endMonth.label}`} />
      </div>

      {/* Status Breakdown */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '16px', marginBottom: '24px' }}>
        <StatusCard icon="✅" label="Delivered" percentage={formatPercent(summary.deliveryRate)} count={`(${formatNumber(summary.delivered)})`} color="success" />
        <StatusCard icon="❌" label="Cancelled" percentage={formatPercent(summary.cancelRate)} count={`(${formatNumber(summary.cancelled)})`} color="danger" />
        <StatusCard icon="↩️" label="Returned" percentage={formatPercent(summary.returnRate)} count={`(${formatNumber(summary.returned)})`} color="warning" />
        <StatusCard icon="⚠️" label="Failed" percentage={formatPercent(summary.failedRate)} count={`(${formatNumber(summary.failed)})`} color="purple" />
      </div>

      {/* Charts Row 1 */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '16px', marginBottom: '24px' }}>
        <Card title="📈 Monthly Order Volume">
          <TrendChart data={trendData} />
        </Card>
        <Card title="📊 Status Distribution">
          <StatusPieChart data={pieData} />
        </Card>
      </div>

      {/* Cancellation Reasons */}
      <Card title="❌ Top Cancellation Reasons">
        {aggregatedReasons.length > 0 ? (
          <HorizontalBarChart data={aggregatedReasons} />
        ) : (
          <p style={{ color: '#888', textAlign: 'center', padding: '40px' }}>No cancellation data for selected range</p>
        )}
      </Card>
    </div>
  );
}
