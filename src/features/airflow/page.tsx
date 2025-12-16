import { useQuery } from '@tanstack/react-query'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { AirflowDagsTable } from './components/airflow-dags-table'
import { AirflowDagsDrawer } from './components/airflow-dags-drawer'
import { AirflowDagsProvider } from './components/airflow-dags-provider'
import { fetchAirflowDags, type AirflowListDagsResponse } from '@/services/airflow'

export function AirflowDagsPage() {
  const { data, isLoading, isError, error } = useQuery<
    AirflowListDagsResponse,
    Error
  >({
    queryKey: ['airflow', 'dags'],
    queryFn: () => fetchAirflowDags({ limit: 500, offset: 0 }),
  })

  if (isLoading) return <div>Loading Airflow DAGs...</div>

  if (isError) {
    const message = error instanceof Error ? error.message : 'Unknown error'
    return <div>Error loading DAGs: {message}</div>
  }

  const dags = data?.dags ?? []
  const total = data?.total_entries ?? 0

  return (
    <AirflowDagsProvider>
      <>
        <Header fixed>
          <Search />
          <div className='ms-auto flex items-center space-x-4'>
            <ThemeSwitch />
            <ConfigDrawer />
            <ProfileDropdown />
          </div>
        </Header>

        <Main className='flex flex-1 flex-col gap-4 p-4 sm:gap-6'>
          <div className='flex flex-wrap items-end justify-between gap-2'>
            <div>
              <h2 className='text-2xl font-bold tracking-tight'>Data Processing</h2>
              <p className='text-muted-foreground'>
                Here&apos;s a list of your Airflow DAGs.
              </p>
            </div>
          </div>

          <div className='flex flex-1 flex-col gap-4'>
            <div className='flex items-center justify-between text-sm text-muted-foreground'>
              <div>
                Showing {dags.length} of {total} DAGs
              </div>
            </div>

            <AirflowDagsTable data={dags} />
          </div>
        </Main>

        <AirflowDagsDrawer />
      </>
    </AirflowDagsProvider>
  )
}
