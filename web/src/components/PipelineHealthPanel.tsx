import type { DlqDepthResponse } from '@/api/types'

interface PipelineHealthPanelProps {
  data: DlqDepthResponse
}

function topicLabel(status: string, depth: number | null): string {
  if (status === 'unavailable' || depth === null) return 'Broker unreachable'
  if (depth === 0) return 'OK'
  return `Warning: ${depth} messages`
}

export default function PipelineHealthPanel({ data }: PipelineHealthPanelProps) {
  return (
    <section aria-label="Pipeline health">
      <h2>Pipeline Health</h2>
      <ul>
        {data.topics.map((t) => (
          <li key={t.topic}>
            <span>{t.topic}: </span>
            <span>{topicLabel(t.status, t.depth)}</span>
          </li>
        ))}
      </ul>
    </section>
  )
}
