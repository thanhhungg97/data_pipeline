export interface MonthlyData {
  Year: number;
  Month: number;
  total_orders: number;
  delivered: number;
  cancelled: number;
  returned: number;
  failed: number;
  Source?: string;
  delivery_rate?: number;
  cancel_rate?: number;
}

export interface ReasonData {
  Year: number;
  Month: number;
  'Reason cancelled': string;
  count: number;
}

export interface SourceMetrics {
  Year: number;
  Month: number;
  Source: string;
  total_orders: number;
  delivered: number;
  cancelled: number;
  returned: number;
  failed: number;
  delivery_rate: number;
  cancel_rate: number;
}

export interface DashboardData {
  monthly: MonthlyData[];
  reasons: ReasonData[];
  sources: string[];
  metrics: SourceMetrics[];
}

export interface SelectOption {
  value: string;
  label: string;
  color?: string;
}

