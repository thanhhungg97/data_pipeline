import { NavLink } from 'react-router-dom';
import { BarChart3, TrendingUp, Store } from 'lucide-react';

const navItems = [
  { to: '/', label: 'Overview', icon: BarChart3 },
  { to: '/compare', label: 'Monthly Compare', icon: TrendingUp },
  { to: '/sources', label: 'Sources', icon: Store },
];

export function Navigation() {
  return (
    <nav style={{ 
      background: 'rgba(0,0,0,0.3)', 
      borderBottom: '1px solid rgba(255,255,255,0.1)', 
      padding: '12px 24px' 
    }}>
      <div style={{ display: 'flex', gap: '8px' }}>
        {navItems.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            style={({ isActive }) => ({
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '8px 16px',
              borderRadius: '8px',
              fontSize: '14px',
              fontWeight: 500,
              textDecoration: 'none',
              transition: 'all 0.2s',
              background: isActive ? '#00d4aa' : 'transparent',
              color: isActive ? 'white' : '#888',
            })}
          >
            <Icon size={18} />
            {label}
          </NavLink>
        ))}
      </div>
    </nav>
  );
}
