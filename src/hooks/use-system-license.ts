import { useQuery, type UseQueryOptions } from '@tanstack/react-query'
import { fetchSystemLicense, type SystemLicenseResponse } from '@/services/system-settings'

type SystemLicenseQueryKey = readonly ['system-license']

type SystemLicenseQueryOptions = Omit<
  UseQueryOptions<SystemLicenseResponse, Error, SystemLicenseResponse, SystemLicenseQueryKey>,
  'queryKey' | 'queryFn'
>

export function useSystemLicenseQuery(options?: SystemLicenseQueryOptions) {
  return useQuery<SystemLicenseResponse, Error, SystemLicenseResponse, SystemLicenseQueryKey>({
    queryKey: ['system-license'],
    queryFn: fetchSystemLicense,
    staleTime: 5 * 60 * 1000,
    ...options,
  })
}
