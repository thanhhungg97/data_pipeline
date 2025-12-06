export const MONTH_NAMES = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function formatNumber(n: number | undefined): string {
  return n !== undefined ? n.toLocaleString() : '0';
}

export function formatPercent(n: number | undefined): string {
  return n !== undefined ? `${n.toFixed(1)}%` : '0%';
}

export function formatMonthYear(year: number, month: number): string {
  return `${MONTH_NAMES[month]} ${year}`;
}

export function getLastMonth(year: number, month: number): { year: number; month: number } {
  return month === 1 ? { year: year - 1, month: 12 } : { year, month: month - 1 };
}

interface ChangeResult {
  value: string;
  isPositive: boolean;
  isNeutral: boolean;
}

export function calculateChange(current: number, previous: number | undefined, isRate = false): ChangeResult {
  if (previous === undefined || previous === 0) {
    return { value: 'N/A', isPositive: false, isNeutral: true };
  }

  const change = isRate
    ? (current - previous).toFixed(1)
    : (((current - previous) / previous) * 100).toFixed(1);

  const numChange = parseFloat(change);

  return {
    value: `${numChange > 0 ? '↑' : '↓'} ${Math.abs(numChange)}${isRate ? 'pp' : '%'}`,
    isPositive: numChange > 0,
    isNeutral: numChange === 0,
  };
}

// Color palette for sources
export const SOURCE_COLORS = [
  '#ee4d2d', // Orange (Shopee)
  '#00d4aa', // Teal
  '#ff6b9d', // Pink
  '#ffa726', // Amber
  '#42a5f5', // Blue
  '#ab47bc', // Purple
  '#26a69a', // Cyan
  '#ef5350', // Red
  '#7e57c2', // Deep purple
  '#66bb6a', // Green
];

export function getSourceColor(index: number): string {
  return SOURCE_COLORS[index % SOURCE_COLORS.length];
}

