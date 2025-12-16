import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { ConfigDrawer } from '@/components/config-drawer'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Button } from '@/components/ui/button'
import {
  fetchPgDatasetTables,
  fetchPgDatasetTableRows,
  type PgDatasetTable,
  type PgDatasetTableRowsResponse,
} from '@/services/datasets'

export const Route = createFileRoute('/_authenticated/datasets/$datasetId/pg')({
  component: PgDatasetBrowserPage,
})

function PgDatasetBrowserPage() {
  const { datasetId } = Route.useParams()
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [page, setPage] = useState(0)

  const {
    data: tables = [],
    isLoading: tablesLoading,
    isError: tablesError,
  } = useQuery<PgDatasetTable[]>({
    queryKey: ['pg-dataset-tables', datasetId],
    queryFn: () => fetchPgDatasetTables(datasetId),
  })

  useEffect(() => {
    if (!selectedTable && tables.length > 0) {
      setSelectedTable(tables[0]?.name ?? null)
    }
  }, [selectedTable, tables])

  useEffect(() => {
    setPage(0)
  }, [selectedTable])

  const {
    data: tableData,
    isLoading: rowsLoading,
    isError: rowsError,
  } = useQuery<PgDatasetTableRowsResponse>({
    queryKey: ['pg-dataset-table-rows', datasetId, selectedTable, page],
    enabled: !!selectedTable,
    queryFn: () =>
      fetchPgDatasetTableRows(datasetId, selectedTable as string, {
        limit: 100,
        offset: page * 100,
      }),
  })

  const totalPages = tableData ? Math.max(Math.ceil(tableData.total / tableData.limit), 1) : 1

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
        <div className='flex flex-wrap items-end justify-between gap-2'>
          <div>
            <h2 className='text-2xl font-bold tracking-tight'>PostgreSQL 数据浏览</h2>
            <p className='text-muted-foreground'>
              数据集 ID：{datasetId}，仅支持 type = 'pgsql' 的团队数据集。
            </p>
          </div>
        </div>

        <div className='flex min-h-0 flex-1 gap-4 overflow-hidden'>
          <div className='w-60 flex-shrink-0 rounded-lg border bg-background p-2'>
            <div className='mb-2 text-sm font-medium'>数据表</div>
            {tablesLoading ? (
              <div className='text-xs text-muted-foreground'>加载表列表...</div>
            ) : tablesError ? (
              <div className='text-xs text-destructive'>加载表列表失败。</div>
            ) : tables.length === 0 ? (
              <div className='text-xs text-muted-foreground'>当前数据库中还没有可用的表。</div>
            ) : (
              <ul className='flex max-h-[calc(100vh-220px)] flex-col gap-1 overflow-auto text-xs'>
                {tables.map((t) => {
                  const active = t.name === selectedTable
                  return (
                    <li key={`${t.schema}.${t.name}`}>
                      <button
                        type='button'
                        onClick={() => setSelectedTable(t.name)}
                        className={`flex w-full items-center justify-between rounded px-2 py-1 text-left hover:bg-muted ${active ? 'bg-muted font-medium' : ''}`}
                      >
                        <span className='truncate'>{t.name}</span>
                        <span className='text-muted-foreground ms-1 shrink-0 text-[10px]'>
                          {t.schema}
                        </span>
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>

          <div className='flex min-w-0 flex-1 flex-col rounded-lg border bg-background p-2'>
            {!selectedTable ? (
              <div className='flex flex-1 items-center justify-center text-sm text-muted-foreground'>
                请先在左侧选择一张表。
              </div>
            ) : rowsLoading ? (
              <div className='flex flex-1 items-center justify-center text-sm text-muted-foreground'>
                加载数据中...
              </div>
            ) : rowsError ? (
              <div className='flex flex-1 items-center justify-center text-sm text-destructive'>
                加载表数据失败。
              </div>
            ) : !tableData || tableData.rows.length === 0 ? (
              <div className='flex flex-1 items-center justify-center text-sm text-muted-foreground'>
                当前表没有数据。
              </div>
            ) : (
              <>
                <div className='mb-2 flex items-center justify-between text-xs text-muted-foreground'>
                  <div>
                    表：<span className='font-mono text-foreground'>{selectedTable}</span>，共 {tableData.total} 行
                  </div>
                  <div className='flex items-center gap-2'>
                    <Button
                      variant='outline'
                      size='icon'
                      className='h-7 w-7'
                      disabled={page <= 0}
                      onClick={() => setPage((p) => Math.max(p - 1, 0))}
                    >
                      {'<'}
                    </Button>
                    <span>
                      第 {page + 1} / {totalPages} 页
                    </span>
                    <Button
                      variant='outline'
                      size='icon'
                      className='h-7 w-7'
                      disabled={page + 1 >= totalPages}
                      onClick={() => setPage((p) => p + 1)}
                    >
                      {'>'}
                    </Button>
                  </div>
                </div>

                <div className='flex-1 overflow-auto rounded-md border bg-background'>
                  <table className='w-full border-collapse text-xs'>
                    <thead className='sticky top-0 z-10 bg-muted'>
                      <tr>
                        {Object.keys(tableData.rows[0] ?? {}).map((key) => (
                          <th key={key} className='border-b px-2 py-1 text-left font-medium'>
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {tableData.rows.map((row, rowIndex) => (
                        <tr key={rowIndex} className='hover:bg-muted/40'>
                          {Object.keys(tableData.rows[0] ?? {}).map((key) => (
                            <td key={key} className='border-b px-2 py-1 align-top'>
                              <span className='break-all font-mono text-[11px]'>
                                {formatCellValue(row[key as keyof typeof row])}
                              </span>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      </Main>
    </>
  )
}

function formatCellValue(value: unknown): string {
  if (value == null) return 'NULL'
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}
