import { useEffect, useState } from 'react'
import { Outlet, useRouter } from '@tanstack/react-router'
import { getCookie } from '@/lib/cookies'
import { cn } from '@/lib/utils'
import { useAuthStore } from '@/stores/auth-store'
import { fetchCurrentUser } from '@/services/auth'
import { LayoutProvider } from '@/context/layout-provider'
import { SearchProvider } from '@/context/search-provider'
import { SidebarInset, SidebarProvider } from '@/components/ui/sidebar'
import { AppSidebar } from '@/components/layout/app-sidebar'
import { SkipToMain } from '@/components/skip-to-main'

type AuthenticatedLayoutProps = {
  children?: React.ReactNode
}

export function AuthenticatedLayout({ children }: AuthenticatedLayoutProps) {
  const router = useRouter()
  const { auth } = useAuthStore()
  const [isCheckingAuth, setIsCheckingAuth] = useState(true)
  const defaultOpen = getCookie('sidebar_state') !== 'false'

  useEffect(() => {
    const checkAuth = async () => {
      if (!auth.accessToken) {
        const redirect = `${window.location.pathname}${window.location.search}`
        router.navigate({ to: '/sign-in', search: { redirect }, replace: true })
        return
      }

      if (!auth.user) {
        try {
          const user = await fetchCurrentUser()
          const exp = Date.now() + 24 * 60 * 60 * 1000
          auth.setUser({
            ...user,
            exp,
          })
        } catch (error) {
          // eslint-disable-next-line no-console
          console.error(error)
          auth.reset()
          const redirect = `${window.location.pathname}${window.location.search}`
          router.navigate({
            to: '/sign-in',
            search: { redirect },
            replace: true,
          })
          return
        }
      }

      setIsCheckingAuth(false)
    }

    void checkAuth()
  }, [auth, router])

  if (isCheckingAuth) {
    return null
  }
  return (
    <SearchProvider>
      <LayoutProvider>
        <SidebarProvider defaultOpen={defaultOpen}>
          <SkipToMain />
          <AppSidebar />
          <SidebarInset
            className={cn(
              // Set content container, so we can use container queries
              '@container/content',

              // If layout is fixed, set the height
              // to 100svh to prevent overflow
              'has-data-[layout=fixed]:h-svh',

              // If layout is fixed and sidebar is inset,
              // set the height to 100svh - spacing (total margins) to prevent overflow
              'peer-data-[variant=inset]:has-data-[layout=fixed]:h-[calc(100svh-(var(--spacing)*4))]'
            )}
          >
            {children ?? <Outlet />}
          </SidebarInset>
        </SidebarProvider>
      </LayoutProvider>
    </SearchProvider>
  )
}
