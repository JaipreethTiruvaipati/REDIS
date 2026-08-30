export function StatusDot({ status, label }: { status: 'online' | 'offline' | 'loading' | 'warning'; label: string }) {
  return <span className="status-item"><span className={`status-dot ${status}`} />{label}</span>
}
