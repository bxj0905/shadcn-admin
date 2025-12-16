import { useEffect, useRef } from 'react'
import { useSystemSettingsQuery } from '@/hooks/use-system-settings'

export function SystemTitleSync() {
  const { data, embeddedBrowserIcon } = useSystemSettingsQuery({
    refetchOnWindowFocus: false,
  })
  const defaultTitleRef = useRef<string | null>(null)
  const defaultFaviconRef = useRef<string | null>(null)

  useEffect(() => {
    if (defaultTitleRef.current === null) {
      defaultTitleRef.current = document.title || 'Shadcn Admin'
    }
    if (defaultFaviconRef.current === null) {
      const existingIcon = document.querySelector<HTMLLinkElement>("link[rel*='icon']")
      defaultFaviconRef.current = existingIcon?.getAttribute('href') ?? null
    }
  }, [])

  useEffect(() => {
    if (defaultTitleRef.current === null) {
      defaultTitleRef.current = document.title || 'Shadcn Admin'
    }

    const name = data?.name?.trim() || defaultTitleRef.current
    const tagline = data?.tagline?.trim()

    document.title = tagline ? `${name} · ${tagline}` : name
  }, [data?.name, data?.tagline])

  useEffect(() => {
    const ensureFaviconEl = () => {
      let link = document.querySelector<HTMLLinkElement>("link[rel*='icon']")
      if (!link) {
        link = document.createElement('link')
        link.rel = 'icon'
        document.head.append(link)
      }
      return link
    }

    const normalizedBrowserIconUrl = data?.browserIconUrl?.trim() || null
    const embeddedMatchesRemote =
      embeddedBrowserIcon &&
      normalizedBrowserIconUrl &&
      embeddedBrowserIcon.sourceUrl === normalizedBrowserIconUrl

    const iconCandidates = [
      embeddedMatchesRemote ? embeddedBrowserIcon?.dataUrl ?? null : normalizedBrowserIconUrl,
      !embeddedMatchesRemote ? embeddedBrowserIcon?.dataUrl ?? null : null,
      defaultFaviconRef.current,
    ]

    const nextIcon = iconCandidates.find((href) => !!href) ?? null

    if (nextIcon) {
      const link = ensureFaviconEl()
      link.href = nextIcon
      if (embeddedMatchesRemote && embeddedBrowserIcon?.mimeType) {
        link.type = embeddedBrowserIcon.mimeType
      } else {
        link.removeAttribute('type')
      }
    } else {
      const link = document.querySelector<HTMLLinkElement>("link[rel*='icon']")
      link?.remove()
    }
  }, [data?.browserIconUrl, embeddedBrowserIcon?.dataUrl, embeddedBrowserIcon?.mimeType, embeddedBrowserIcon?.sourceUrl])

  return null
}
