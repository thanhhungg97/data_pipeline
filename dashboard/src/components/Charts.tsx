import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  AreaChart,
} from 'recharts';

const COLORS = {
  primary: '#00d4aa',
  secondary: '#4ecdc4',
  danger: '#ff6b6b',
  warning: '#ffa726',
  purple: '#ab47bc',
  blue: '#42a5f5',
};

const STATUS_COLORS = ['#00d4aa', '#ff6b6b', '#ffa726', '#ab47bc'];

interface TrendChartProps {
  data: { name: string; value: number; delivered?: number }[];
  showDelivered?: boolean;
}

export function TrendChart({ data, showDelivered = false }: TrendChartProps) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <AreaChart data={data}>
        <defs>
          <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={COLORS.primary} stopOpacity={0.3} />
            <stop offset="95%" stopColor={COLORS.primary} stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
        <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} angle={-45} textAnchor="end" height={60} />
        <YAxis stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
        <Tooltip
          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 8 }}
          labelStyle={{ color: '#fff' }}
        />
        <Legend />
        <Area type="monotone" dataKey="value" name="Total Orders" stroke={COLORS.primary} fill="url(#colorValue)" strokeWidth={3} />
        {showDelivered && (
          <Line type="monotone" dataKey="delivered" name="Delivered" stroke={COLORS.secondary} strokeWidth={2} strokeDasharray="5 5" dot={false} />
        )}
      </AreaChart>
    </ResponsiveContainer>
  );
}

interface PieChartProps {
  data: { name: string; value: number }[];
}

export function StatusPieChart({ data }: PieChartProps) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          innerRadius={60}
          outerRadius={100}
          paddingAngle={2}
          dataKey="value"
          label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
          labelLine={{ stroke: '#888' }}
        >
          {data.map((_, index) => (
            <Cell key={`cell-${index}`} fill={STATUS_COLORS[index % STATUS_COLORS.length]} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 8 }}
        />
      </PieChart>
    </ResponsiveContainer>
  );
}

interface BarChartProps {
  data: { name: string; [key: string]: string | number }[];
  bars: { key: string; color: string; name: string }[];
}

export function ComparisonBarChart({ data, bars }: BarChartProps) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
        <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
        <YAxis stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
        <Tooltip
          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 8 }}
          labelStyle={{ color: '#fff' }}
        />
        <Legend />
        {bars.map((bar) => (
          <Bar key={bar.key} dataKey={bar.key} name={bar.name} fill={bar.color} radius={[4, 4, 0, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

interface HorizontalBarChartProps {
  data: { name: string; value: number }[];
  color?: string;
}

export function HorizontalBarChart({ data, color = COLORS.danger }: HorizontalBarChartProps) {
  return (
    <ResponsiveContainer width="100%" height={Math.max(300, data.length * 40)}>
      <BarChart data={data} layout="vertical" margin={{ left: 150 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
        <XAxis type="number" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
        <YAxis type="category" dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} width={140} />
        <Tooltip
          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 8 }}
        />
        <Bar dataKey="value" fill={color} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

interface MultiLineChartProps {
  data: { name: string; [key: string]: string | number }[];
  lines: { key: string; color: string; name: string }[];
}

export function MultiLineChart({ data, lines }: MultiLineChartProps) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
        <XAxis dataKey="name" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} angle={-45} textAnchor="end" height={60} />
        <YAxis stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
        <Tooltip
          contentStyle={{ background: '#1a1a2e', border: '1px solid rgba(255,255,255,0.2)', borderRadius: 8 }}
          labelStyle={{ color: '#fff' }}
        />
        <Legend />
        {lines.map((line) => (
          <Line key={line.key} type="monotone" dataKey={line.key} name={line.name} stroke={line.color} strokeWidth={3} dot={{ r: 4 }} />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

