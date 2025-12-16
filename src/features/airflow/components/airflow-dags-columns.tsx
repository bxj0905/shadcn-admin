import { type ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { DataTableColumnHeader } from '@/components/data-table'
import type { AirflowDag } from '@/services/airflow'
import { AirflowDagsRowActions } from './airflow-dags-row-actions'

export const airflowDagsColumns: ColumnDef<AirflowDag>[] = [
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
    accessorKey: 'dag_id',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='DAG ID' />
    ),
    cell: ({ row }) => (
      <div className='w-[80px]'>{row.getValue('dag_id')}</div>
    ),
    enableSorting: true,
    enableHiding: false,
  },
  {
    accessorKey: 'description',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Description' />
    ),
    meta: { className: 'ps-1', tdClassName: 'ps-4' },
    cell: ({ row }) => {
      const value = row.getValue<string | null>('description') ?? ''
      return (
        <span className='max-w-72 truncate' title={value}>
          {value}
        </span>
      )
    },
  },
  {
    accessorKey: 'is_paused',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Paused' />
    ),
    meta: { className: 'ps-1', tdClassName: 'ps-4' },
    cell: ({ row }) => {
      const paused = row.getValue<boolean>('is_paused')
      return <span>{paused ? 'Yes' : 'No'}</span>
    },
  },
  {
    accessorKey: 'next_dagrun',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Next Run' />
    ),
    meta: { className: 'ps-1', tdClassName: 'ps-4' },
    cell: ({ row }) => {
      const value = row.getValue<string | null>('next_dagrun')
      return <span>{value ?? '-'}</span>
    },
  },
  {
    id: 'tags',
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title='Tags' />
    ),
    meta: { className: 'ps-1', tdClassName: 'ps-4' },
    cell: ({ row }) => {
      const tags = (row.original.tags ?? []) as { name: string }[]
      if (!tags.length) return null
      return (
        <div className='flex flex-wrap gap-1'>
          {tags.map((tag) => (
            <Badge key={tag.name} variant='outline' className='text-xs font-normal'>
              {tag.name}
            </Badge>
          ))}
        </div>
      )
    },
  },
  {
    id: 'actions',
    cell: ({ row }) => <AirflowDagsRowActions row={row} />,
  },
]
