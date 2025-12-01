import { useEffect, useMemo, useState } from 'react'
import {
  type ColumnDef,
  type SortingState,
  type VisibilityState,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { cn } from '@/lib/utils'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  DataTableColumnHeader,
  DataTablePagination,
  DataTableToolbar,
} from '@/components/data-table'
import type { TeamMember } from '@/services/teams'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

type TeamMembersTableProps = {
  data: TeamMember[]
  onRemove: (member: TeamMember) => void
  onRoleChange: (
    member: TeamMember,
    role: 'owner' | 'maintainer' | 'member',
  ) => void
}

export function TeamMembersTable({ data, onRemove, onRoleChange }: TeamMembersTableProps) {
  const [sorting, setSorting] = useState<SortingState>([])
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})

  const [globalFilter, setGlobalFilter] = useState('')
  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize: 10 })

  const columns = useMemo<ColumnDef<TeamMember>[]>(
    () => [
      {
        accessorKey: 'username',
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title='用户名' />
        ),
        cell: ({ row }) => <span>{row.original.username}</span>,
      },
      {
        accessorKey: 'name',
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title='姓名' />
        ),
        cell: ({ row }) => <span>{row.original.name || '-'}</span>,
      },
      {
        accessorKey: 'teamRoleCode',
        header: ({ column }) => (
          <DataTableColumnHeader column={column} title='角色' />
        ),
        cell: ({ row }) => (
          <Select
            value={row.original.teamRoleCode || 'member'}
            onValueChange={(value) =>
              onRoleChange(
                row.original,
                value as 'owner' | 'maintainer' | 'member',
              )
            }
          >
            <SelectTrigger className='h-8 w-28 text-xs'>
              <SelectValue
                placeholder={
                  row.original.teamRoleName || row.original.teamRoleCode || '成员'
                }
              />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value='owner'>拥有者</SelectItem>
              <SelectItem value='maintainer'>维护者</SelectItem>
              <SelectItem value='member'>成员</SelectItem>
            </SelectContent>
          </Select>
        ),
      },
      {
        id: 'actions',
        header: '操作',
        cell: ({ row }) => (
          <div className='text-right'>
            <Button
              variant='ghost'
              size='sm'
              onClick={() => onRemove(row.original)}
              className='text-muted-foreground hover:text-red-500 hover:bg-red-500/10'
            >
              移除
            </Button>
          </div>
        ),
        meta: {
          className: 'w-24 text-right',
          tdClassName: 'text-right',
        },
      },
    ],
    [onRemove, onRoleChange],
  )

  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnVisibility,
      globalFilter,
      pagination,
    },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    onGlobalFilterChange: setGlobalFilter,
    onPaginationChange: setPagination,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    globalFilterFn: 'includesString',
  })

  useEffect(() => {
    if (table.getState().pagination.pageIndex > table.getPageCount() - 1) {
      table.setPageIndex(0)
    }
  }, [table])

  return (
    <div className='flex flex-1 flex-col gap-4'>
      <DataTableToolbar
        table={table}
        searchPlaceholder='搜索成员...'
        searchKey='username'
      />
      <div className='overflow-hidden rounded-md border'>
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className='group/row'>
                {headerGroup.headers.map((header) => (
                  <TableHead
                    key={header.id}
                    colSpan={header.colSpan}
                    className={cn(
                      'bg-background group-hover/row:bg-muted group-data-[state=selected]/row:bg-muted',
                      header.column.columnDef.meta?.className,
                      header.column.columnDef.meta?.thClassName,
                    )}
                  >
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id} className='group/row'>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell
                      key={cell.id}
                      className={cn(
                        'bg-background group-hover/row:bg-muted group-data-[state=selected]/row:bg-muted',
                        cell.column.columnDef.meta?.className,
                        cell.column.columnDef.meta?.tdClassName,
                      )}
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={4} className='h-24 text-center'>
                  暂无成员。
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <DataTablePagination table={table} className='mt-auto' />
    </div>
  )
}
