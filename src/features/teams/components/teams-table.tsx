import { useState } from 'react'
import {
  type SortingState,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  createColumnHelper,
} from '@tanstack/react-table'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { DataTablePagination } from '@/components/data-table'
import { cn } from '@/lib/utils'
import type { Team } from '../data/schema'
import { useTeams } from './teams-provider'
import { useAuthStore } from '@/stores/auth-store'
import { deleteTeam } from '@/services/teams'
import { useQueryClient } from '@tanstack/react-query'
import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

const columnHelper = createColumnHelper<Team>()

function createColumns(onEditTeam: (team: Team) => void, onDeleteTeam: (team: Team) => void) {
  return [
    columnHelper.accessor('name', {
      header: '团队名称',
      cell: (info) => info.getValue(),
    }),
    columnHelper.accessor('ownerUsername', {
      header: '所有者用户名',
      cell: (info) => info.getValue(),
    }),
    columnHelper.accessor('ownerName', {
      header: '所有者姓名',
      cell: (info) => info.getValue() || '-',
    }),
    columnHelper.display({
      id: 'actions',
      header: '操作',
      cell: ({ row }) => {
        const team = row.original
        return (
          <div className='flex justify-end'>
            <DropdownMenu modal={false}>
              <DropdownMenuTrigger asChild>
                <Button
                  variant='ghost'
                  className='data-[state=open]:bg-muted flex h-8 w-8 p-0'
                >
                  <DotsHorizontalIcon className='h-4 w-4' />
                  <span className='sr-only'>打开菜单</span>
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align='end' className='w-[160px]'>
                <DropdownMenuItem
                  onClick={() => {
                    onEditTeam(team)
                  }}
                >
                  编辑团队
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  onClick={() => onDeleteTeam(team)}
                  className='text-red-500 focus:text-red-500'
                >
                  删除
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        )
      },
      meta: {
        className: 'w-60 text-right',
        tdClassName: 'text-right',
      },
    }),
  ]
}

type TeamsTableProps = {
  data: Team[]
}

export function TeamsTable({ data }: TeamsTableProps) {
  const { setOpen, setCurrentTeam } = useTeams()
  const { auth } = useAuthStore()
  const queryClient = useQueryClient()

  const currentUser = auth.user
  const isSuperAdmin = !!currentUser && currentUser.roles.includes('super_admin')
  const currentUserId = currentUser?.id

  const handleEditTeam = (team: Team) => {
    setCurrentTeam(team)
    setOpen('add')
  }

  const handleDeleteTeam = async (team: Team) => {
    if (!currentUser) return

    const canDelete =
      isSuperAdmin || (currentUserId !== undefined && team.ownerId === currentUserId)

    if (!canDelete) {
      // 简单前端保护，后端也会校验
      return
    }

    // eslint-disable-next-line no-alert
    const confirmed = window.confirm('确认删除该团队？此操作不可恢复。')
    if (!confirmed) return

    await deleteTeam(team.id)
    await queryClient.invalidateQueries({ queryKey: ['teams'] })
  }

  const [sorting, setSorting] = useState<SortingState>([])

  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns: createColumns(handleEditTeam, handleDeleteTeam),
    state: {
      sorting,
    },
    onSortingChange: setSorting,
    getPaginationRowModel: getPaginationRowModel(),
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })

  return (
    <div className='flex flex-1 flex-col gap-4'>
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
                      header.column.columnDef.meta?.thClassName
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
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  data-state={row.getIsSelected() && 'selected'}
                  className='group/row'
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell
                      key={cell.id}
                      className={cn(
                        'bg-background group-hover/row:bg-muted group-data-[state=selected]/row:bg-muted',
                        cell.column.columnDef.meta?.className,
                        cell.column.columnDef.meta?.tdClassName
                      )}
                    >
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className='h-24 text-center'>
                  暂无团队。
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
