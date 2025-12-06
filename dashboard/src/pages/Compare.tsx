import { Card, StatCard, StatusCard } from '../components/Card';
import { ComparisonBarChart, TrendChart } from '../components/Charts';
import { MONTH_NAMES, calculateChange, formatMonthYear, formatNumber, formatPercent, getLastMonth } from '../utils/format';
import { SelectOption, SourceMetrics } from '../types';
import { useMemo, useState } from 'react';

import Select from 'react-select';

interface CompareProps {
  data: SourceMetrics[];
  sources: string[];
}

export function Compare({ data, sources }: CompareProps) {
  const sourceOptions = useMemo(() => [
    { value: 'All', label: '📊 All Sources' },
    ...sources.map((s) => ({ value: s, label: s })),
  ], [sources]);

  const [selectedSource, setSelectedSource] = useState<SelectOption>(sourceOptions[0]);

  const filteredData = useMemo(() => {
    const source = selectedSource.value;
    return source === 'All'
      ? data.filter((d) => d.Source === 'All')
      : data.filter((d) => d.Source === source);
  }, [data, selectedSource]);

  const sortedData = useMemo(() =>
    [...filteredData].sort((a, b) => a.Year !== b.Year ? a.Year - b.Year : a.Month - b.Month),
    [filteredData]
  );

  const monthOptions = useMemo(() =>
    [...sortedData].reverse().map((d) => ({
      value: `${d.Year}-${d.Month}`,
      label: formatMonthYear(d.Year, d.Month),
    })),
    [sortedData]
  );

  const [selectedMonth, setSelectedMonth] = useState<SelectOption | null>(monthOptions[0] || null);

  const currentData = useMemo(() => {
    if (!selectedMonth) return null;
    const [year, month] = selectedMonth.value.split('-').map(Number);
    return filteredData.find((d) => d.Year === year && d.Month === month) || null;
  }, [filteredData, selectedMonth]);

  const lastMonthData = useMemo(() => {
    if (!selectedMonth) return null;
    const [year, month] = selectedMonth.value.split('-').map(Number);
    const { year: ly, month: lm } = getLastMonth(year, month);
    return filteredData.find((d) => d.Year === ly && d.Month === lm) || null;
  }, [filteredData, selectedMonth]);

  const lastYearData = useMemo(() => {
    if (!selectedMonth) return null;
    const [year, month] = selectedMonth.value.split('-').map(Number);
    return filteredData.find((d) => d.Year === year - 1 && d.Month === month) || null;
  }, [filteredData, selectedMonth]);

  const trendData = useMemo(() => {
    if (!selectedMonth) return [];
    const [year, month] = selectedMonth.value.split('-').map(Number);
    const currentIdx = sortedData.findIndex((d) => d.Year === year && d.Month === month);
    const slice = sortedData.slice(Math.max(0, currentIdx - 11), currentIdx + 1);
    return slice.map((d) => ({
      name: `${MONTH_NAMES[d.Month]} ${d.Year}`,
      value: d.total_orders,
      delivered: d.delivered,
    }));
  }, [sortedData, selectedMonth]);

  const comparisonData = useMemo(() => {
    if (!currentData) return [];
    return [
      {
        name: 'Total',
        current: currentData.total_orders,
        lastMonth: lastMonthData?.total_orders || 0,
        lastYear: lastYearData?.total_orders || 0,
      },
      {
        name: 'Delivered',
        current: currentData.delivered,
        lastMonth: lastMonthData?.delivered || 0,
        lastYear: lastYearData?.delivered || 0,
      },
      {
        name: 'Cancelled',
        current: currentData.cancelled,
        lastMonth: lastMonthData?.cancelled || 0,
        lastYear: lastYearData?.cancelled || 0,
      },
      {
        name: 'Returned',
        current: currentData.returned,
        lastMonth: lastMonthData?.returned || 0,
        lastYear: lastYearData?.returned || 0,
      },
    ];
  }, [currentData, lastMonthData, lastYearData]);

  const renderChange = (change: ReturnType<typeof calculateChange>, invertColor = false) => {
    if (change.isNeutral) return <span className="text-gray-500">{change.value}</span>;
    const colorClass = invertColor
      ? change.isPositive ? 'text-red-400' : 'text-emerald-400'
      : change.isPositive ? 'text-emerald-400' : 'text-red-400';
    return <span className={colorClass}>{change.value}</span>;
  };

  if (!currentData) {
    return <div className="p-6 text-white">Loading...</div>;
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-white">📈 Monthly Comparison Dashboard</h1>
        <p className="text-gray-400 mt-1">Compare current month with previous month and year-over-year</p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-4">
        <div>
          <label className="block text-gray-400 text-xs uppercase tracking-wider mb-2">Source</label>
          <Select
            value={selectedSource}
            onChange={(v) => v && setSelectedSource(v)}
            options={sourceOptions}
            className="w-56"
            classNamePrefix="react-select"
          />
        </div>
        <div>
          <label className="block text-gray-400 text-xs uppercase tracking-wider mb-2">Month</label>
          <Select
            value={selectedMonth}
            onChange={(v) => v && setSelectedMonth(v)}
            options={monthOptions}
            className="w-48"
            classNamePrefix="react-select"
          />
        </div>
      </div>

      {/* Main Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatCard
          label="Total Orders"
          value={formatNumber(currentData.total_orders)}
          comparisons={[
            { label: 'vs Last Month', value: renderChange(calculateChange(currentData.total_orders, lastMonthData?.total_orders)) },
            { label: 'vs Last Year', value: renderChange(calculateChange(currentData.total_orders, lastYearData?.total_orders)) },
          ]}
        />
        <StatCard
          label="Delivery Rate"
          value={formatPercent(currentData.delivery_rate)}
          color="success"
          comparisons={[
            { label: 'vs Last Month', value: renderChange(calculateChange(currentData.delivery_rate, lastMonthData?.delivery_rate, true)) },
            { label: 'vs Last Year', value: renderChange(calculateChange(currentData.delivery_rate, lastYearData?.delivery_rate, true)) },
          ]}
        />
        <StatCard
          label="Cancellation Rate"
          value={formatPercent(currentData.cancel_rate)}
          color="danger"
          comparisons={[
            { label: 'vs Last Month', value: renderChange(calculateChange(currentData.cancel_rate, lastMonthData?.cancel_rate, true), true) },
            { label: 'vs Last Year', value: renderChange(calculateChange(currentData.cancel_rate, lastYearData?.cancel_rate, true), true) },
          ]}
        />
      </div>

      {/* Status Breakdown */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatusCard icon="✅" label="Delivered" percentage={formatPercent(currentData.delivery_rate)} count={`(${formatNumber(currentData.delivered)})`} color="success" />
        <StatusCard icon="❌" label="Cancelled" percentage={formatPercent(currentData.cancel_rate)} count={`(${formatNumber(currentData.cancelled)})`} color="danger" />
        <StatusCard
          icon="↩️"
          label="Returned"
          percentage={formatPercent(currentData.total_orders ? (currentData.returned / currentData.total_orders) * 100 : 0)}
          count={`(${formatNumber(currentData.returned)})`}
          color="warning"
        />
        <StatusCard
          icon="⚠️"
          label="Failed"
          percentage={formatPercent(currentData.total_orders ? (currentData.failed / currentData.total_orders) * 100 : 0)}
          count={`(${formatNumber(currentData.failed)})`}
          color="purple"
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <Card title="📈 Orders Trend (12 Months)" className="lg:col-span-3">
          <TrendChart data={trendData} showDelivered />
        </Card>
        <Card title="📊 3-Month Comparison" className="lg:col-span-2">
          <ComparisonBarChart
            data={comparisonData}
            bars={[
              { key: 'current', color: '#00d4aa', name: selectedMonth?.label || 'Current' },
              { key: 'lastMonth', color: '#4a90a4', name: 'Last Month' },
              { key: 'lastYear', color: '#8b5cf6', name: 'Last Year' },
            ]}
          />
        </Card>
      </div>
    </div>
  );
}

