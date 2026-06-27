import { useQuery } from '@tanstack/react-query'
import { fetchDlqDepth } from '@/api/client'
import type { DlqDepthResponse } from '@/api/types'

export function useDlqDepth() {
  return useQuery<DlqDepthResponse, Error>({
    queryKey: ['dlq-depth'],
    queryFn: fetchDlqDepth,
  })
}
