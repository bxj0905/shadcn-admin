import { type ColumnDef, type CellContext } from '@tanstack/react-table'
import { cn } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { DataTableColumnHeader } from '@/components/data-table'
import { LongText } from '@/components/long-text'
import { callTypes, roles } from '../data/data'
import { type User } from '../data/schema'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { updateUserStatus } from '@/services/users'
import { DataTableRowActions } from './data-table-row-actions'

function StatusCell({ row }: CellContext<User, unknown>) {
  const { id, status } = row.original
  const badgeColor =
    status === 'active'
      ? 'bg-green-500 text-white border-transparent'
      : status === 'inactive'
        ? 'bg-red-500 text-white border-transparent'
        : callTypes.get(status)

  const queryClient = useQueryClient()
  const mutation = useMutation({
    mutationFn: (nextStatus: 'active' | 'inactive') => {
      const numeric = nextStatus === 'active' ? 1 : 0
      return updateUserStatus(id, numeric)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
    },
  })

  const handleToggle = () => {
    const next = status === 'active' ? 'inactive' : 'active'
    mutation.mutate(next)
  }

  const displayLabel = status === 'active' ? '启用' : status === 'inactive' ? '禁用' : row.getValue('status')

  return (
    <button
      type='button'
      onClick={handleToggle}
      className='inline-flex items-center w-[72px] justify-center'
      disabled={mutation.status === 'pending'}
    >
      <Badge
        variant='outline'
        className={cn('capitalize w-full justify-center', badgeColor)}
      >
        {displayLabel}
      </Badge>
    </button>
  )
}

export const usersColumns: ColumnDef<User>[] = [
  {
    id: 'select',
    header: ({ table }) => (
      <Checkbox
        checked={
          table.getIsAllPageRowsSelected() ||
          (table.getIsSomePageRowsSelected() && 'indeterminate')
        }
        onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
        aria-label='Select all'
        className='translate-y-[2px]'
      />
    ),
    meta: {
      className: cn('max-md:sticky start-0 z-10 rounded-tl-[inherit]'),
    },
    cell: ({ row }) => (
      <Checkbox
        checked={row.getIsSelected()}
        onCheckedChange={(value) => row.toggleSelected(!!value)}
        aria-label='Select row'
        className='translate-y-[2px]'
      />
    ),
    enableSorting: false,
    enableHiding: false,
  },
  {
    accessorKey: 'username',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='用户名' />
    ),
    cell: ({ row }) => (
      <LongText className='max-w-36 ps-3'>{row.getValue('username')}</LongText>
    ),
    meta: {
      className: cn(
        'drop-shadow-[0_1px_2px_rgb(0_0_0_/_0.1)] dark:drop-shadow-[0_1px_2px_rgb(255_255_255_/_0.1)]',
        'ps-0.5 max-md:sticky start-6 @4xl/content:table-cell @4xl/content:drop-shadow-none'
      ),
    },
    enableHiding: false,
  },
  {
    id: 'fullName',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='姓名' />
    ),
    cell: ({ row }) => {
      const { firstName, lastName } = row.original
      const fullName = `${firstName} ${lastName}`
      return <LongText className='max-w-36'>{fullName}</LongText>
    },
    meta: { className: 'w-36' },
  },
  {
    accessorKey: 'email',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='邮箱' />
    ),
    cell: ({ row }) => (
      <div className='w-fit ps-2 text-nowrap'>{row.getValue('email')}</div>
    ),
  },
  {
    accessorKey: 'status',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='状态' />
    ),
    cell: StatusCell,
    filterFn: (row, id, value) => {
      return value.includes(row.getValue(id))
    },
    enableHiding: false,
    enableSorting: false,
  },
  {
    accessorKey: 'accountSource',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='账号来源' />
    ),
    cell: ({ row }) => (
      <span className='text-xs text-muted-foreground'>
        {row.getValue('accountSource')}
      </span>
    ),
    enableSorting: false,
  },
  {
    accessorKey: 'role',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='角色' />
    ),
    cell: ({ row }) => {
      const { role, roleName } = row.original
      const userType = roles.find(({ value }) => value === role)

      const displayLabel = roleName || userType?.label || row.getValue('role')

      if (!displayLabel) {
        return null
      }

      return (
        <div className='flex items-center gap-x-2'>
          {userType?.icon && (
            <userType.icon size={16} className='text-muted-foreground' />
          )}
          <span className='text-sm'>{displayLabel}</span>
        </div>
      )
    },
    filterFn: (row, id, value) => {
      return value.includes(row.getValue(id))
    },
    enableSorting: false,
    enableHiding: false,
  },
  {
    id: 'actions',
    cell: DataTableRowActions,
  },
]
