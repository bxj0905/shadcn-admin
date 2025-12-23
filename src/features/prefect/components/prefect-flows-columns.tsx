import { type ColumnDef } from '@tanstack/react-table'
import { type TFunction } from 'i18next'
import { DataTableColumnHeader } from '@/components/data-table'
import { cn } from '@/lib/utils'
import type { PrefectFlow } from '@/services/prefect'
import { PrefectFlowsRowActions } from './prefect-flows-row-actions'

function formatIsoToSeconds(value?: string | null) {
  if (!value) return '-'
  try {
    // 转为本地时间（例如浏览器所在时区，北京时间），再格式化到秒
    const d = new Date(value)
    if (Number.isNaN(d.getTime())) {
      return value.replace(/\.\d+Z?$/, '')
    }
    const y = d.getFullYear()
    const m = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    const hh = String(d.getHours()).padStart(2, '0')
    const mm = String(d.getMinutes()).padStart(2, '0')
    const ss = String(d.getSeconds()).padStart(2, '0')
    // 中间用空格而不是 T，便于阅读：YYYY-MM-DD HH:mm:ss
    return `${y}-${m}-${day} ${hh}:${mm}:${ss}`
  } catch {
    // 解析失败时退回简单处理：只去掉毫秒和 Z
    return value.replace(/\.\d+Z?$/, '')
  }
}

export function getPrefectFlowsColumns(
  t: TFunction<'common'>,
): ColumnDef<PrefectFlow>[] {
  return [
    {
      id: 'status-indicator',
      header: () => null,
      // 注意：状态指示器的实时查询由上层组件（如行动作或独立 hook）负责；
      // 这里保持静态样式，避免在非组件函数中直接使用 React Hook。
      cell: () => (
        <div className='flex justify-start'>
          <span className='inline-block h-8 w-1.5 rounded-full bg-muted-foreground/30' />
        </div>
      ),
      meta: { className: 'ps-0', tdClassName: 'ps-0' },
      enableSorting: false,
      enableHiding: false,
    },
    {
      accessorKey: 'name',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={t('prefect.table.columns.name')}
        />
      ),
      cell: ({ row }) => {
        const flow = row.original as PrefectFlow
        const isSubflow = flow.isSubflow

        return (
          <div
            className={cn(
              'flex w-[200px] items-center gap-2',
              isSubflow && 'ps-6 text-muted-foreground',
            )}
          >
            {isSubflow && (
              <span className='text-xs leading-none text-muted-foreground'>↳</span>
            )}
            <span className='truncate'>{row.getValue('name') as string}</span>
          </div>
        )
      },
      enableSorting: true,
      enableHiding: false,
    },
    {
      id: 'latestRun',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={t('prefect.table.columns.lastRun')}
          className='justify-start'
        />
      ),
      meta: { className: 'ps-1 text-left', tdClassName: 'ps-4 text-left' },
      // 为避免在 cell 中直接使用 React Hook，这里改为仅展示占位符文本；
      // 真实的“最新运行时间”信息可在后续单独组件中以 Hook 方式实现。
      cell: () => (
        <span className='text-xs text-muted-foreground'>
          {t('prefect.table.lastRun.loading')}
        </span>
      ),
    },
    {
      accessorKey: 'created',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={t('prefect.table.columns.created')}
          className='justify-start'
        />
      ),
      meta: { className: 'ps-1 text-left', tdClassName: 'ps-4 text-left' },
      cell: ({ row }) => {
        const value = row.getValue<string | null>('created')
        const formatted = formatIsoToSeconds(value)
        return <span>{formatted}</span>
      },
    },
    {
      accessorKey: 'updated',
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={t('prefect.table.columns.updated')}
          className='justify-start'
        />
      ),
      meta: { className: 'ps-1 text-left', tdClassName: 'ps-4 text-left' },
      cell: ({ row }) => {
        const value = row.getValue<string | null>('updated')
        const formatted = formatIsoToSeconds(value)
        return <span>{formatted}</span>
      },
    },
    {
      id: 'actions',
      cell: ({ row }) => <PrefectFlowsRowActions row={row} />,
    },
  ]
}
