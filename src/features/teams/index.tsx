import { getRouteApi } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { TeamsDialogs } from './components/teams-dialogs'
import { TeamsPrimaryButtons } from './components/teams-primary-buttons'
import { TeamsProvider } from './components/teams-provider'
import { TeamsTable } from './components/teams-table'
import { fetchTeams } from '@/services/teams'

const route = getRouteApi('/_authenticated/teams/')

export function Teams() {
  route.useSearch()

  const { data: teams = [], isLoading, isError } = useQuery({
    queryKey: ['teams'],
    queryFn: fetchTeams,
  })

  return (
    <TeamsProvider>
      <Header fixed>
        <Search />
        <div className='ms-auto flex items-center space-x-4'>
          <ThemeSwitch />
          <ConfigDrawer />
          <ProfileDropdown />
        </div>
      </Header>

      <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>团队列表</h2>
            <p className='text-muted-foreground'>在这里管理团队和成员。</p>
          </div>
          <TeamsPrimaryButtons />
        </div>
        {isError ? (
          <p className='text-destructive text-sm'>Failed to load teams.</p>
        ) : isLoading ? (
          <p className='text-sm text-muted-foreground'>Loading teams...</p>
        ) : (
          <TeamsTable data={teams} />
        )}
      </Main>

      <TeamsDialogs />
    </TeamsProvider>
  )
}
