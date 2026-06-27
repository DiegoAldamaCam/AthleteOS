// CoachingFlagsPanel renders badge chips for each active coaching flag.
// Null or empty array -> renders nothing (no visible output).
//
// Spec: metrics-v2 coaching_flags capability (obs #121 Scenarios 12-16).

interface CoachingFlagsPanelProps {
  coaching_flags: string[] | null
}

// Human-readable label and accessible color hint per flag.
const FLAG_CONFIG: Record<string, { label: string; colorClass: string }> = {
  deload: { label: 'Deload', colorClass: 'badge-deload' },
  undertrained: { label: 'Undertrained', colorClass: 'badge-undertrained' },
  high_fatigue: { label: 'High Fatigue', colorClass: 'badge-high-fatigue' },
  monitor: { label: 'Monitor', colorClass: 'badge-monitor' },
}

export default function CoachingFlagsPanel({ coaching_flags }: CoachingFlagsPanelProps) {
  if (!coaching_flags || coaching_flags.length === 0) {
    return null
  }

  return (
    <div role="list" aria-label="Coaching flags">
      {coaching_flags.map((flag) => {
        const config = FLAG_CONFIG[flag] ?? { label: flag, colorClass: 'badge-default' }
        return (
          <span
            key={flag}
            role="listitem"
            className={`coaching-badge ${config.colorClass}`}
            aria-label={config.label}
          >
            {config.label}
          </span>
        )
      })}
    </div>
  )
}
