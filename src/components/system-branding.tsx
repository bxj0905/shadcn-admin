import { useEffect, useState } from 'react'
import { cn } from '@/lib/utils'
import { useSystemSettingsQuery } from '@/hooks/use-system-settings'

type SystemBrandingProps = {
  orientation?: 'row' | 'column'
  showTagline?: boolean
  showName?: boolean
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

const containerOrientation = {
  row: 'flex-row text-left',
  column: 'flex-col text-center',
}

const avatarSizeMap = {
  sm: 'size-8 text-sm',
  md: 'size-10 text-base',
  lg: 'size-12 text-lg',
}

const textSizeMap = {
  sm: {
    name: 'text-sm font-semibold',
    tagline: 'text-xs',
  },
  md: {
    name: 'text-base font-semibold',
    tagline: 'text-sm',
  },
  lg: {
    name: 'text-lg font-semibold',
    tagline: 'text-base',
  },
}

export function SystemBranding({
  orientation = 'row',
  showTagline = false,
  showName = true,
  size = 'md',
  className,
}: SystemBrandingProps) {
  const { data, embeddedLogo } = useSystemSettingsQuery()
  const name = data?.name ?? 'Shadcn Admin'
  const tagline = data?.tagline ?? 'Vite + ShadcnUI Starter'
  const logoUrl = data?.logoUrl ?? null
  const hasEmbeddedLogo = Boolean(embeddedLogo?.dataUrl)
  const initials = name?.charAt(0)?.toUpperCase() ?? 'S'
  const [useEmbeddedLogo, setUseEmbeddedLogo] = useState(false)
  const [logoCompletelyFailed, setLogoCompletelyFailed] = useState(false)

  useEffect(() => {
    if (useEmbeddedLogo || logoCompletelyFailed) {
      setUseEmbeddedLogo(false)
      setLogoCompletelyFailed(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [logoUrl, embeddedLogo?.dataUrl])

  const logoSrc = useEmbeddedLogo
    ? embeddedLogo?.dataUrl ?? null
    : logoUrl ?? embeddedLogo?.dataUrl ?? null

  return (
    <div
      className={cn(
        'flex items-center gap-3',
        containerOrientation[orientation],
        className,
      )}
    >
      {logoSrc && !logoCompletelyFailed ? (
        <img
          src={logoSrc}
          alt={`${name} logo`}
          className={cn(
            'rounded-xl border bg-card object-cover',
            avatarSizeMap[size],
          )}
          onError={() => {
            if (!useEmbeddedLogo && hasEmbeddedLogo) {
              setUseEmbeddedLogo(true)
            } else {
              setLogoCompletelyFailed(true)
            }
          }}
        />
      ) : (
        <div
          className={cn(
            'flex items-center justify-center rounded-xl border bg-card font-semibold text-primary',
            avatarSizeMap[size],
          )}
        >
          {initials}
        </div>
      )}

      {showName && (
        <div className='space-y-0.5'>
          <p className={cn(textSizeMap[size].name)}>{name}</p>
          {showTagline && tagline && (
            <p className={cn('text-muted-foreground', textSizeMap[size].tagline)}>
              {tagline}
            </p>
          )}
        </div>
      )}
    </div>
  )
}
