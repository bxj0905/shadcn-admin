import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Textarea } from '@/components/ui/textarea'
import {
  fetchPgDatasetTables,
  fetchPgDatasetTableRows,
  executePgDatasetQuery,
  type Dataset,
  type PgDatasetTable,
  type PgDatasetTableRowsResponse,
  type PgDatasetQueryResult,
} from '@/services/datasets'

interface PgDatasetDialogProps {
  dataset: Dataset | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function PgDatasetDialog({ dataset, open, onOpenChange }: PgDatasetDialogProps) {
  const datasetId = dataset?.id
  const [selectedTableName, setSelectedTableName] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [sqlInput, setSqlInput] = useState('')
  const [queryResult, setQueryResult] = useState<PgDatasetQueryResult | null>(null)
  const [queryError, setQueryError] = useState<string | null>(null)

  const canEdit = dataset?.permission === 'admin' || dataset?.permission === 'editor'

  useEffect(() => {
    if (!open) {
      setSelectedTableName(null)
      setPage(0)
      setSqlInput('')
      setQueryResult(null)
      setQueryError(null)
    }
  }, [open])

  const {
    data: tables = [],
    isLoading: tablesLoading,
    isError: tablesError,
  } = useQuery<PgDatasetTable[]>({
    queryKey: ['pg-dataset-tables', datasetId],
    queryFn: () => fetchPgDatasetTables(String(datasetId)),
    enabled: open && !!datasetId,
    staleTime: 60_000, // Changed from 30_000 to 60_000
  })

  const selectedTable = useMemo(() => {
    if (!tables.length) return null
    if (selectedTableName && tables.some((t) => t.name === selectedTableName)) {
      return selectedTableName
    }
    return tables[0]?.name ?? null
  }, [selectedTableName, tables])

  useEffect(() => {
    setPage(0)
  }, [selectedTable])

  const {
    data: tableData,
    isLoading: rowsLoading,
    isError: rowsError,
  } = useQuery<PgDatasetTableRowsResponse>({
    queryKey: ['pg-dataset-table-rows', datasetId, selectedTable, page],
    enabled: open && !!datasetId && !!selectedTable,
    queryFn: () =>
      fetchPgDatasetTableRows(String(datasetId), selectedTable as string, {
        limit: 100,
        offset: page * 100,
      }),
    keepPreviousData: true,
  })

  const totalPages = tableData ? Math.max(Math.ceil(tableData.total / tableData.limit), 1) : 1

  useEffect(() => {
    if (selectedTable && !sqlInput) {
      setSqlInput(`SELECT * FROM ${selectedTable} LIMIT 100;`)
    }
  }, [selectedTable, sqlInput])

  const { mutateAsync: runQuery, isPending: runningQuery } = useMutation({
    mutationFn: async () => {
      if (!datasetId) return null
      setQueryError(null)
      const trimmed = sqlInput.trim()
      if (!trimmed) {
        setQueryError('请输入 SQL 语句')
        return null
      }
      const result = await executePgDatasetQuery(datasetId, {
        sql: trimmed,
      })
      setQueryResult(result)
      return result
    },
    onError: (error: unknown) => {
      const message = error instanceof Error ? error.message : '执行 SQL 失败'
      setQueryError(message)
    },
  })

  const handleRunQuery = async () => {
    await runQuery()
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className='max-w-6xl gap-4 p-0 sm:gap-4 sm:p-0'>
        <DialogHeader className='px-6 pt-5'>
          <DialogTitle className='text-2xl'>PostgreSQL 数据浏览</DialogTitle>
          <DialogDescription className='text-sm text-muted-foreground'>
            数据集：{dataset?.name}{' '}
            <Badge variant='outline' className='ms-2'>
              {dataset?.permission === 'admin'
                ? '管理员'
                : dataset?.permission === 'editor'
                  ? '可编辑'
                  : dataset?.permission === 'viewer'
                    ? '只读'
                    : '无权限'}
            </Badge>
          </DialogDescription>
        </DialogHeader>

        <div className='grid gap-4 px-6 pb-6 lg:grid-cols-[220px,1fr]'>
          <div className='rounded-lg border bg-muted/40 p-3'>
            <h4 className='mb-2 text-sm font-semibold'>数据表</h4>
            <ScrollArea className='h-[360px] pr-2 text-xs'>
              {tablesLoading ? (
                <p className='text-muted-foreground'>加载表列表...</p>
              ) : tablesError ? (
                <p className='text-destructive'>加载表列表失败</p>
              ) : tables.length === 0 ? (
                <p className='text-muted-foreground'>当前数据库没有可用的表</p>
              ) : (
                <ul className='space-y-1'>
                  {tables.map((table) => {
                    const active = table.name === selectedTable
                    return (
                      <li key={`${table.schema}.${table.name}`}>
                        <button
                          type='button'
                          onClick={() => setSelectedTableName(table.name)}
                          className={`flex w-full items-center justify-between rounded-md px-2 py-1 text-left transition ${
                            active ? 'bg-primary/10 font-medium' : 'hover:bg-muted'
                          }`}
                        >
                          <span className='truncate'>{table.name}</span>
                          <span className='ms-2 text-[10px] uppercase text-muted-foreground'>{table.schema}</span>
                        </button>
                      </li>
                    )
                  })}
                </ul>
              )}
            </ScrollArea>
          </div>

          <div className='flex flex-col gap-4'>
            <div className='rounded-lg border p-3'>
              <div className='mb-2 flex items-center justify-between text-xs text-muted-foreground'>
                <div>
                  表：<span className='font-mono text-sm text-foreground'>{selectedTable ?? '无'}</span>
                </div>
                <div className='flex items-center gap-2'>
                  <Button
                    variant='outline'
                    size='icon'
                    className='h-7 w-7'
                    disabled={page <= 0 || rowsLoading}
                    onClick={() => setPage((prev) => Math.max(prev - 1, 0))}
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
                    disabled={rowsLoading || page + 1 >= totalPages}
                    onClick={() => setPage((prev) => prev + 1)}
                  >
                    {'>'}
                  </Button>
                </div>
              </div>

              <div className='h-[280px] overflow-auto rounded border'>
                {!selectedTable ? (
                  <p className='flex h-full items-center justify-center text-sm text-muted-foreground'>
                    请选择要查看的表
                  </p>
                ) : rowsLoading ? (
                  <p className='flex h-full items-center justify-center text-sm text-muted-foreground'>
                    加载数据中...
                  </p>
                ) : rowsError ? (
                  <p className='flex h-full items-center justify-center text-sm text-destructive'>
                    加载表数据失败
                  </p>
                ) : !tableData || tableData.rows.length === 0 ? (
                  <p className='flex h-full items-center justify-center text-sm text-muted-foreground'>
                    当前表没有数据
                  </p>
                ) : (
                  <table className='w-full border-collapse text-xs'>
                    <thead className='sticky top-0 bg-muted/70'>
                      <tr>
                        {Object.keys(tableData.rows[0]).map((key) => (
                          <th key={key} className='border-b px-2 py-1 text-left font-medium'>
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {tableData.rows.map((row, rowIdx) => (
                        <tr key={rowIdx} className='odd:bg-background even:bg-muted/20'>
                          {Object.keys(tableData.rows[0]).map((key) => (
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
                )}
              </div>
            </div>

            <div className='rounded-lg border p-3'>
              <div className='mb-2 flex items-center justify-between'>
                <div>
                  <h4 className='text-sm font-semibold'>SQL Console</h4>
                  <p className='text-xs text-muted-foreground'>
                    {canEdit ? '可执行任意 SQL，请谨慎操作。' : '当前权限仅允许查看数据。'}
                  </p>
                </div>
                <Button
                  size='sm'
                  onClick={handleRunQuery}
                  disabled={!canEdit && sqlInput.trim().toLowerCase().startsWith('select') === false}
                  variant='secondary'
                >
                  {runningQuery ? '执行中...' : '运行 SQL'}
                </Button>
              </div>
              <Textarea
                value={sqlInput}
                onChange={(e) => setSqlInput(e.target.value)}
                placeholder='输入 SQL，如 SELECT * FROM table LIMIT 100;'
                className='font-mono text-sm'
                rows={5}
                disabled={!datasetId}
              />
              {queryError && <p className='mt-2 text-xs text-destructive'>{queryError}</p>}
              {queryResult && (
                <div className='mt-3 text-xs text-muted-foreground'>
                  <p>
                    {queryResult.command} · 影响行数：{queryResult.rowCount}{' '}
                    {typeof queryResult.durationMs === 'number' && `· ${queryResult.durationMs}ms`}
                  </p>
                  {queryResult.rows.length > 0 && (
                    <div className='mt-2 max-h-52 overflow-auto rounded border bg-background'>
                      <table className='w-full border-collapse text-xs'>
                        <thead className='sticky top-0 bg-muted/70'>
                          <tr>
                            {queryResult.columns.map((col) => (
                              <th key={col} className='border-b px-2 py-1 text-left font-medium'>
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {queryResult.rows.map((row, idx) => (
                            <tr key={idx} className='odd:bg-background even:bg-muted/20'>
                              {queryResult.columns.map((col) => (
                                <td key={`${idx}-${col}`} className='border-b px-2 py-1 align-top'>
                                  <span className='break-all font-mono text-[11px]'>
                                    {formatCellValue(row[col as keyof typeof row])}
                                  </span>
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  )
}

function formatCellValue(value: unknown): string {
  if (value == null) return 'NULL'
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  try {
    return JSON.stringify(value)
  } catch (err) {
    return String(value)
  }
}
