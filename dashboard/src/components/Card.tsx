import { ReactNode, CSSProperties } from 'react';

interface CardProps {
  children: ReactNode;
  className?: string;
  title?: string;
  style?: CSSProperties;
}

const cardStyle: CSSProperties = {
  background: 'rgba(255, 255, 255, 0.05)',
  border: '1px solid rgba(255, 255, 255, 0.1)',
  borderRadius: '12px',
  padding: '20px',
};

export function Card({ children, title, style }: CardProps) {
  return (
    <div style={{ ...cardStyle, ...style }}>
      {title && <h3 style={{ color: 'white', fontWeight: 600, marginBottom: '16px' }}>{title}</h3>}
      {children}
    </div>
  );
}

interface StatCardProps {
  label: string;
  value: string | number;
  subValue?: string;
  color?: 'default' | 'success' | 'danger' | 'warning' | 'purple';
  comparisons?: { label: string; value: ReactNode }[];
}

const colorMap = {
  default: '#fff',
  success: '#00d4aa',
  danger: '#ff6b6b',
  warning: '#ffa726',
  purple: '#ab47bc',
};

export function StatCard({ label, value, subValue, color = 'default', comparisons }: StatCardProps) {
  return (
    <Card>
      <div style={{ color: '#888', fontSize: '14px', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: '8px' }}>{label}</div>
      <div style={{ fontSize: '36px', fontWeight: 700, color: colorMap[color] }}>{value}</div>
      {subValue && <div style={{ color: '#666', fontSize: '14px', marginTop: '4px' }}>{subValue}</div>}
      {comparisons && comparisons.length > 0 && (
        <div style={{ display: 'flex', gap: '16px', marginTop: '16px' }}>
          {comparisons.map((c, i) => (
            <div key={i}>
              <div style={{ color: '#666', fontSize: '12px' }}>{c.label}</div>
              <div style={{ fontSize: '14px', fontWeight: 500 }}>{c.value}</div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

interface StatusCardProps {
  icon: string;
  label: string;
  percentage: string;
  count: string;
  color: 'success' | 'danger' | 'warning' | 'purple';
}

export function StatusCard({ icon, label, percentage, count, color }: StatusCardProps) {
  return (
    <Card>
      <div style={{ color: '#888', fontSize: '14px', marginBottom: '8px' }}>{icon} {label}</div>
      <span style={{ fontSize: '28px', fontWeight: 700, color: colorMap[color] }}>{percentage}</span>
      <span style={{ color: '#666', marginLeft: '8px' }}>{count}</span>
    </Card>
  );
}
