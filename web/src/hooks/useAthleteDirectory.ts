import { useQuery } from '@tanstack/react-query'
import { fetchAthleteDirectory } from '@/api/client'
import type { AthleteDirectoryEntry } from '@/api/types'

/**
 * Fetches the athlete directory (id + name + sport) used by the searchable,
 * sport-filterable athlete picker. Falls back gracefully: entries with null
 * sport are grouped under "Other".
 */
export function useAthleteDirectory() {
  return useQuery<AthleteDirectoryEntry[], Error>({
    queryKey: ['athlete-directory'],
    queryFn: fetchAthleteDirectory,
  })
}
