import { useState, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { useTeamStore } from '@/stores/team-store'
import { fetchTeamDatasets, type Dataset, type DatasetPermission } from '@/services/datasets'
import { DatasetsCreateDialog } from '@/features/datasets/widgets/datasets-create-dialog'
import { DatasetsMembersDialog } from '@/features/datasets/widgets/datasets-members-dialog'
import { DatasetsImportDialog } from '@/features/datasets/widgets/datasets-import-dialog'

function permissionLabel(permission: DatasetPermission) {
  if (permission === 'admin') return '管理员'
  if (permission === 'editor') return '可编辑'
  if (permission === 'viewer') return '只读'
  return '无权限'
}

export function DatasetsPage() {
  const { currentTeam } = useTeamStore()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [membersDataset, setMembersDataset] = useState<Dataset | null>(null)
  const [importDataset, setImportDataset] = useState<Dataset | null>(null)

  const teamId = currentTeam?.id

  const {
    data: datasets = [],
    isLoading,
    isError,
  } = useQuery<Dataset[]>({
    queryKey: ['team-datasets', teamId],
    queryFn: () => fetchTeamDatasets(String(teamId)),
    enabled: !!teamId,
  })

  const canCreate = useMemo(() => {
    if (!datasets.length) return true
    // 有任何一个数据集是 admin 权限，就认为可以创建（团队 owner/maintainer）
    return datasets.some((d) => d.permission === 'admin')
  }, [datasets])

  const onCreated = async () => {
    if (!teamId) return
    await queryClient.invalidateQueries({ queryKey: ['team-datasets', teamId] })
  }

  if (!teamId) {
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
        <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
          <div className='flex flex-col gap-2'>
            <h2 className='text-2xl font-bold tracking-tight'>数据集</h2>
            <p className='text-muted-foreground'>请先在左侧顶部选择一个团队。</p>
          </div>
        </Main>
      </>
    )
  }

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

      <Main className='flex flex-1 flex-col gap-4 sm:gap-6'>
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>数据集</h2>
            <p className='text-muted-foreground'>基于当前团队管理 DuckDB / PostgreSQL 数据集。</p>
          </div>
          {canCreate && (
            <Button size='sm' onClick={() => setCreateOpen(true)}>
              新建数据集
            </Button>
          )}
        </div>

        {isError ? (
          <p className='text-destructive text-sm'>加载数据集失败。</p>
        ) : isLoading ? (
          <div className='grid gap-4 sm:grid-cols-2 xl:grid-cols-3'>
            {Array.from({ length: 6 }).map((_, i) => (
              <Card key={i} className='h-full'>
                <CardHeader>
                  <Skeleton className='h-5 w-32' />
                  <Skeleton className='mt-2 h-4 w-40' />
                </CardHeader>
                <CardContent className='space-y-2'>
                  <Skeleton className='h-4 w-24' />
                  <Skeleton className='h-4 w-32' />
                  <Skeleton className='h-4 w-20' />
                </CardContent>
              </Card>
            ))}
          </div>
        ) : datasets.length === 0 ? (
          <div className='flex flex-1 flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center'>
            <p className='text-lg font-medium'>当前团队还没有数据集</p>
            <p className='text-muted-foreground text-sm'>你可以创建一个 DuckDB 或 PostgreSQL 数据集。</p>
            {canCreate && (
              <Button size='sm' className='mt-2' onClick={() => setCreateOpen(true)}>
                新建数据集
              </Button>
            )}
          </div>
        ) : (
          <div className='grid gap-4 sm:grid-cols-2 xl:grid-cols-3'>
            {datasets.map((dataset) => (
              <Card key={dataset.id} className='flex h-full flex-col justify-between'>
                <CardHeader>
                  <div className='flex items-center justify-between gap-2'>
                    <div>
                      <CardTitle className='line-clamp-1'>{dataset.name}</CardTitle>
                      {dataset.description && (
                        <CardDescription className='mt-1 line-clamp-2'>
                          {dataset.description}
                        </CardDescription>
                      )}
                    </div>
                    <div className='flex flex-col items-end gap-2'>
                      {dataset.type === 'duckdb' ? (
                        <div className='flex h-9 w-9 items-center justify-center rounded-md bg-primary/10'>
                          <img
                            src='/images/DuckDB_icon-lightmode.svg'
                            alt='DuckDB'
                            className='block h-7 w-7 dark:hidden'
                          />
                          <img
                            src='/images/DuckDB_icon-darkmode.svg'
                            alt='DuckDB'
                            className='hidden h-7 w-7 dark:block'
                          />
                        </div>
                      ) : (
                        <div className='flex h-9 w-9 items-center justify-center rounded-md bg-primary/10'>
                          <img
                            src='/images/postgresql-icon.svg'
                            alt='PostgreSQL'
                            className='h-7 w-7'
                          />
                        </div>
                      )}
                      <Badge variant='outline'>{permissionLabel(dataset.permission)}</Badge>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className='space-y-2 text-sm text-muted-foreground'>
                  <p>存储：{dataset.storagePath}</p>
                  <p>创建时间：{new Date(dataset.createdAt).toLocaleString()}</p>
                  <div className='flex justify-end gap-3 pt-2'>
                    <Button
                      variant='outline'
                      size='sm'
                      onClick={() => setMembersDataset(dataset)}
                      disabled={dataset.permission !== 'admin'}
                    >
                      成员权限
                    </Button>
                    <Button
                      variant='outline'
                      size='sm'
                      onClick={() => setImportDataset(dataset)}
                    >
                      导入数据
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </Main>

      <DatasetsCreateDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        teamId={String(teamId)}
        onCreated={onCreated}
      />

      {membersDataset && (
        <DatasetsMembersDialog
          dataset={membersDataset}
          open={!!membersDataset}
          onOpenChange={(open: boolean) => {
            if (!open) setMembersDataset(null)
          }}
        />
      )}

      {importDataset && (
        <DatasetsImportDialog
          dataset={importDataset}
          open={!!importDataset}
          onOpenChange={(open: boolean) => {
            if (!open) setImportDataset(null)
          }}
        />
      )}
    </>
  )
}
