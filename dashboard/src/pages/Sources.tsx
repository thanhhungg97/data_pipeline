import { useState, useMemo } from 'react';
import Select from 'react-select';
import { Card } from '../components/Card';
import { MultiLineChart, ComparisonBarChart } from '../components/Charts';
import { formatNumber, formatPercent, formatMonthYear, getSourceColor, MONTH_NAMES } from '../utils/format';
import { SourceMetrics, SelectOption } from '../types';

interface SourcesProps {
  data: SourceMetrics[];
  sources: string[];
}

export function Sources({ data, sources }: SourcesProps) {
  const sourceColors = useMemo(() => {
    const colors: Record<string, string> = {};
    sources.forEach((s, i) => {
      colors[s] = getSourceColor(i);
    });
    return colors;
  }, [sources]);

  const sourceOptions = useMemo(() =>
    sources.map((s, i) => ({
      value: s,
      label: s,
      color: getSourceColor(i),
    })),
    [sources]
  );

  const [selectedSources, setSelectedSources] = useState<SelectOption[]>(sourceOptions);

  const periods = useMemo(() => {
    const unique = [...new Set(data.map((d) => `${d.Year}-${d.Month}`))];
    return unique.sort().reverse();
  }, [data]);

  const monthOptions = useMemo(() =>
    periods.map((p) => {
      const [year, month] = p.split('-').map(Number);
      return { value: p, label: formatMonthYear(year, month) };
    }),
    [periods]
  );

  const [selectedMonth, setSelectedMonth] = useState<SelectOption>(monthOptions[0]);

  const selectedSourceValues = selectedSources.map((s) => s.value);

  const getSourceData = (source: string, year: number, month: number) =>
    data.find((d) => d.Source === source && d.Year === year && d.Month === month);

  const [currentYear, currentMonth] = selectedMonth.value.split('-').map(Number);

  const trendData = useMemo(() => {
    const last12 = periods.slice(0, 12).reverse();
    return last12.map((p) => {
      const [year, month] = p.split('-').map(Number);
      const point: Record<string, string | number> = { name: `${MONTH_NAMES[month]} ${year}` };
      selectedSourceValues.forEach((source) => {
        const d = getSourceData(source, year, month);
        point[source] = d?.total_orders || 0;
      });
      return point;
    });
  }, [periods, selectedSourceValues, data]);

  const comparisonData = useMemo(() => {
    const metrics = ['Total Orders', 'Delivered', 'Cancelled', 'Returned'];
    return metrics.map((metric) => {
      const point: Record<string, string | number> = { name: metric };
      selectedSourceValues.forEach((source) => {
        const d = getSourceData(source, currentYear, currentMonth);
        if (!d) {
          point[source] = 0;
        } else {
          switch (metric) {
            case 'Total Orders': point[source] = d.total_orders; break;
            case 'Delivered': point[source] = d.delivered; break;
            case 'Cancelled': point[source] = d.cancelled; break;
            case 'Returned': point[source] = d.returned; break;
          }
        }
      });
      return point;
    });
  }, [selectedSourceValues, currentYear, currentMonth, data]);

  const tableRows = [
    { label: 'Total Orders', key: 'total_orders', best: 'max' },
    { label: 'Delivered', key: 'delivered', best: 'max' },
    { label: 'Delivery Rate', key: 'delivery_rate', best: 'max', suffix: '%' },
    { label: 'Cancelled', key: 'cancelled', best: 'min' },
    { label: 'Cancel Rate', key: 'cancel_rate', best: 'min', suffix: '%' },
    { label: 'Returned', key: 'returned', best: 'min' },
  ];

  const customStyles = {
    multiValue: (base: object, { data }: { data: SelectOption }) => ({
      ...base,
      backgroundColor: data.color,
    }),
    multiValueLabel: (base: object) => ({
      ...base,
      color: 'white',
    }),
    option: (base: object, { data }: { data: SelectOption }) => ({
      ...base,
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      '&::before': {
        content: '""',
        width: '12px',
        height: '12px',
        borderRadius: '50%',
        backgroundColor: data.color,
      },
    }),
  };

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-white">🏪 Multi-Source Comparison Dashboard</h1>
        <p className="text-gray-400 mt-1">Compare performance across different sales channels</p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-4">
        <div>
          <label className="block text-gray-400 text-xs uppercase tracking-wider mb-2">Select Month</label>
          <Select
            value={selectedMonth}
            onChange={(v) => v && setSelectedMonth(v)}
            options={monthOptions}
            className="w-48"
            classNamePrefix="react-select"
          />
        </div>
        <div className="flex-1 min-w-64">
          <label className="block text-gray-400 text-xs uppercase tracking-wider mb-2">Compare Sources</label>
          <Select
            isMulti
            value={selectedSources}
            onChange={(v) => setSelectedSources(v.length > 0 ? [...v] : [sourceOptions[0]])}
            options={sourceOptions}
            className="w-full"
            classNamePrefix="react-select"
            styles={customStyles}
            closeMenuOnSelect={false}
          />
        </div>
      </div>

      {/* Source Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {selectedSourceValues.map((source) => {
          const d = getSourceData(source, currentYear, currentMonth);
          const color = sourceColors[source];
          return (
            <Card key={source}>
              <div className="flex items-center gap-2 mb-4">
                <span className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
                <h3 className="font-semibold" style={{ color }}>{source}</h3>
              </div>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-400">Total Orders</span>
                  <span className="text-white font-semibold">{d ? formatNumber(d.total_orders) : '-'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Delivery Rate</span>
                  <span className="text-emerald-400 font-semibold">{d ? formatPercent(d.delivery_rate) : '-'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Cancel Rate</span>
                  <span className="text-red-400 font-semibold">{d ? formatPercent(d.cancel_rate) : '-'}</span>
                </div>
              </div>
            </Card>
          );
        })}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card title="📈 Orders by Source Over Time">
          <MultiLineChart
            data={trendData}
            lines={selectedSourceValues.map((s) => ({
              key: s,
              color: sourceColors[s],
              name: s,
            }))}
          />
        </Card>
        <Card title="📊 Source Comparison (Selected Month)">
          <ComparisonBarChart
            data={comparisonData}
            bars={selectedSourceValues.map((s) => ({
              key: s,
              color: sourceColors[s],
              name: s,
            }))}
          />
        </Card>
      </div>

      {/* Comparison Table */}
      <Card title="📋 Detailed Comparison">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-white/10">
                <th className="text-left py-3 px-4 text-gray-400 text-xs uppercase tracking-wider">Metric</th>
                {selectedSourceValues.map((s) => (
                  <th key={s} className="text-left py-3 px-4 text-xs uppercase tracking-wider" style={{ color: sourceColors[s] }}>
                    <span className="inline-block w-2 h-2 rounded-full mr-2" style={{ backgroundColor: sourceColors[s] }} />
                    {s}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {tableRows.map((row) => {
                const values = selectedSourceValues.map((s) => {
                  const d = getSourceData(s, currentYear, currentMonth);
                  return d ? (d as unknown as Record<string, number>)[row.key] || 0 : 0;
                });
                const best = row.best === 'max'
                  ? Math.max(...values)
                  : Math.min(...values.filter((v) => v > 0));

                return (
                  <tr key={row.label} className="border-b border-white/5 hover:bg-white/5">
                    <td className="py-3 px-4 text-gray-300">{row.label}</td>
                    {values.map((v, i) => (
                      <td
                        key={i}
                        className={`py-3 px-4 ${v === best && v > 0 ? 'text-emerald-400 font-semibold' : 'text-gray-300'}`}
                      >
                        {formatNumber(v)}{row.suffix || ''}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

