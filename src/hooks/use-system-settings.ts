import { useEffect, useState } from 'react'
import {
  useQuery,
  type UseQueryOptions,
  type UseQueryResult,
} from '@tanstack/react-query'
import { fetchSystemSettings, type SystemSettingsResponse } from '@/services/system-settings'
import {
  loadSystemSettingsCache,
  saveSystemSettingsCache,
  type CachedSystemSettings,
} from '@/lib/system-settings-cache'

type SystemSettingsQueryKey = readonly ['system-settings']

type SystemSettingsQueryOptions = Omit<
  UseQueryOptions<SystemSettingsResponse, Error, SystemSettingsResponse, SystemSettingsQueryKey>,
  'queryKey' | 'queryFn'
>

type UseSystemSettingsQueryResult = UseQueryResult<SystemSettingsResponse, Error> & {
  embeddedLogo: CachedSystemSettings['embeddedLogo'] | null
  embeddedBrowserIcon: CachedSystemSettings['embeddedBrowserIcon'] | null
}

export function useSystemSettingsQuery(options?: SystemSettingsQueryOptions) {
  const cache =
    typeof window !== 'undefined' ? loadSystemSettingsCache() : null
  const cachedPayload = cache?.payload ?? null
  const [isOnline, setIsOnline] = useState(
    typeof navigator !== 'undefined' ? navigator.onLine : true,
  )
  const initialData = options?.initialData ?? cachedPayload ?? undefined
  const initialDataUpdatedAt =
    options?.initialDataUpdatedAt ??
    (cachedPayload ? new Date(cachedPayload.updatedAt).getTime() : undefined)
  const [embeddedLogo, setEmbeddedLogo] = useState<CachedSystemSettings['embeddedLogo'] | null>(
    cache?.embeddedLogo ?? null,
  )
  const [embeddedBrowserIcon, setEmbeddedBrowserIcon] =
    useState<CachedSystemSettings['embeddedBrowserIcon'] | null>(cache?.embeddedBrowserIcon ?? null)

  const queryResult = useQuery<SystemSettingsResponse, Error, SystemSettingsResponse, SystemSettingsQueryKey>({
    ...(options ?? {}),
    queryKey: ['system-settings'] as const,
    queryFn: fetchSystemSettings,
    staleTime: options?.staleTime ?? 5 * 60 * 1000,
    initialData,
    initialDataUpdatedAt,
    enabled: (options?.enabled ?? true) && isOnline,
  })

  useEffect(() => {
    if (!queryResult.data) {
      return
    }

    let cancelled = false

    const persistCache = async () => {
      const latestCache = loadSystemSettingsCache()
      let nextEmbeddedLogo: CachedSystemSettings['embeddedLogo'] | null =
        latestCache?.embeddedLogo ?? null
      let nextEmbeddedBrowserIcon: CachedSystemSettings['embeddedBrowserIcon'] | null =
        latestCache?.embeddedBrowserIcon ?? null
      const logoUrl = queryResult.data?.logoUrl ?? null
      const browserIconUrl = queryResult.data?.browserIconUrl ?? null

      if (logoUrl) {
        const alreadyEmbedded =
          nextEmbeddedLogo && nextEmbeddedLogo.sourceUrl === logoUrl

        if (!alreadyEmbedded) {
          try {
            const response = await fetch(logoUrl, { credentials: 'include' })
            const blob = await response.blob()
            const dataUrl = await new Promise<string>((resolve, reject) => {
              const reader = new FileReader()
              reader.onloadend = () => {
                if (typeof reader.result === 'string') {
                  resolve(reader.result)
                } else {
                  reject(new Error('Failed to convert logo blob to data URL'))
                }
              }
              reader.onerror = () => reject(reader.error ?? new Error('Failed to read logo blob'))
              reader.readAsDataURL(blob)
            })

            nextEmbeddedLogo = {
              sourceUrl: logoUrl,
              dataUrl,
              mimeType: blob.type || undefined,
              savedAt: new Date().toISOString(),
            }
          } catch {
            // Keep previous embedded logo if fetch fails (e.g., offline)
            nextEmbeddedLogo = latestCache?.embeddedLogo ?? nextEmbeddedLogo
          }
        }
      } else {
        nextEmbeddedLogo = null
      }

      if (browserIconUrl) {
        const alreadyEmbedded =
          nextEmbeddedBrowserIcon && nextEmbeddedBrowserIcon.sourceUrl === browserIconUrl

        if (!alreadyEmbedded) {
          try {
            const response = await fetch(browserIconUrl, { credentials: 'include' })
            const blob = await response.blob()
            const dataUrl = await new Promise<string>((resolve, reject) => {
              const reader = new FileReader()
              reader.onloadend = () => {
                if (typeof reader.result === 'string') {
                  resolve(reader.result)
                } else {
                  reject(new Error('Failed to convert browser icon blob to data URL'))
                }
              }
              reader.onerror = () => reject(reader.error ?? new Error('Failed to read browser icon blob'))
              reader.readAsDataURL(blob)
            })

            nextEmbeddedBrowserIcon = {
              sourceUrl: browserIconUrl,
              dataUrl,
              mimeType: blob.type || undefined,
              savedAt: new Date().toISOString(),
            }
          } catch {
            nextEmbeddedBrowserIcon =
              latestCache?.embeddedBrowserIcon ?? nextEmbeddedBrowserIcon
          }
        }
      } else {
        nextEmbeddedBrowserIcon = null
      }

      if (!cancelled) {
        const payloadToPersist: CachedSystemSettings = {
          payload: queryResult.data,
          embeddedLogo: nextEmbeddedLogo,
          embeddedBrowserIcon: nextEmbeddedBrowserIcon,
        }
        saveSystemSettingsCache(payloadToPersist)
        setEmbeddedLogo(payloadToPersist.embeddedLogo ?? null)
        setEmbeddedBrowserIcon(payloadToPersist.embeddedBrowserIcon ?? null)
      }
    }

    void persistCache()

    return () => {
      cancelled = true
    }
  }, [queryResult.data])

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }

    const handleOnline = () => setIsOnline(true)
    const handleOffline = () => setIsOnline(false)

    window.addEventListener('online', handleOnline)
    window.addEventListener('offline', handleOffline)
    return () => {
      window.removeEventListener('online', handleOnline)
      window.removeEventListener('offline', handleOffline)
    }
  }, [])

  const extendedResult: UseSystemSettingsQueryResult = {
    ...queryResult,
    embeddedLogo,
    embeddedBrowserIcon,
  }

  return extendedResult
}
