import type { SystemSettingsResponse } from '@/services/system-settings'

const SYSTEM_SETTINGS_CACHE_KEY = 'system_settings_cache_v2'

export type EmbeddedAsset = {
  sourceUrl: string
  dataUrl: string
  mimeType?: string
  savedAt: string
}

export type CachedSystemSettings = {
  payload: SystemSettingsResponse
  embeddedLogo?: EmbeddedAsset | null
  embeddedBrowserIcon?: EmbeddedAsset | null
}

export function saveSystemSettingsCache(cache: CachedSystemSettings) {
  try {
    localStorage.setItem(SYSTEM_SETTINGS_CACHE_KEY, JSON.stringify(cache))
  } catch {
    // ignore quota/security errors
  }
}

export function loadSystemSettingsCache(): CachedSystemSettings | null {
  try {
    const raw = localStorage.getItem(SYSTEM_SETTINGS_CACHE_KEY)
    if (!raw) {
      return null
    }
    const parsed = JSON.parse(raw) as CachedSystemSettings | SystemSettingsResponse
    if ('payload' in parsed) {
      return parsed as CachedSystemSettings
    }
    return {
      payload: parsed as SystemSettingsResponse,
      embeddedLogo: null,
      embeddedBrowserIcon: null,
    }
  } catch {
    return null
  }
}

export function clearSystemSettingsCache() {
  try {
    localStorage.removeItem(SYSTEM_SETTINGS_CACHE_KEY)
  } catch {
    // ignore
  }
}
