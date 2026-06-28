import { useQuery } from '@tanstack/react-query'
import { fetchAthletes } from '@/api/client'

export function useAthletes() {
  return useQuery<string[], Error>({
    queryKey: ['athletes'],
    queryFn: fetchAthletes,
  })
}
