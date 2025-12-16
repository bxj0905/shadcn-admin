import { createFileRoute } from '@tanstack/react-router'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { ConfigDrawer } from '@/components/config-drawer'

export const Route = createFileRoute('/_authenticated/drizzle-studio')({
  component: DeprecatedDrizzleStudioPage,
})

function DeprecatedDrizzleStudioPage() {
  return (
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
        <div className='flex flex-col gap-2'>
          <h2 className='text-2xl font-bold tracking-tight'>数据浏览入口已更新</h2>
          <p className='text-muted-foreground'>
            原 Drizzle Studio 页面已下线，请在左侧导航进入「Datasets」，然后选择 PostgreSQL 类型的数据集并点击「查看数据」来浏览和查询数据。
          </p>
        </div>
      </Main>
    </>
  )
}
