import { Outlet } from '@tanstack/react-router'
import { useTranslation } from 'react-i18next'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Separator } from '@/components/ui/separator'
import { SidebarNav } from '@/features/settings/components/sidebar-nav'
import { BadgeCheck, ShieldCheck } from 'lucide-react'

const sidebarItems = (t: (key: string) => string) => [
  {
    title: t('systemSettings.branding.title'),
    href: '/system-settings',
    icon: <BadgeCheck size={18} />,
  },
  {
    title: t('systemSettings.license.title'),
    href: '/system-settings/license',
    icon: <ShieldCheck size={18} />,
  },
]

export function SystemSettingsPage() {
  const { t } = useTranslation()

  return (
    <>
      <Header>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          <ConfigDrawer />
          <ProfileDropdown />
        </div>
      </Header>

      <Main fixed>
        <div className='space-y-0.5'>
          <h1 className='text-2xl font-bold tracking-tight md:text-3xl'>
            {t('systemSettings.title')}
          </h1>
          <p className='text-muted-foreground'>{t('systemSettings.description')}</p>
        </div>
        <Separator className='my-4 lg:my-6' />
        <div className='flex flex-1 flex-col space-y-2 overflow-hidden md:space-y-2 lg:flex-row lg:space-y-0 lg:space-x-12'>
          <aside className='top-0 lg:sticky lg:w-1/5'>
            <SidebarNav items={sidebarItems(t)} />
          </aside>
          <div className='flex w-full overflow-y-hidden p-1'>
            <Outlet />
          </div>
        </div>
      </Main>
    </>
  )
}
