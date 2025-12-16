import { DotsHorizontalIcon } from '@radix-ui/react-icons'
import type { Row } from '@tanstack/react-table'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import type { AirflowDag } from '@/services/airflow'
import { useAirflowDags } from './airflow-dags-provider'

interface AirflowDagsRowActionsProps {
  row: Row<AirflowDag>
}

export function AirflowDagsRowActions({ row }: AirflowDagsRowActionsProps) {
  const dag = row.original
  const { openDrawer } = useAirflowDags()

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant='ghost'
          className='flex size-8 p-0 data-[state=open]:bg-muted'
        >
          <DotsHorizontalIcon className='size-4' />
          <span className='sr-only'>Open menu</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align='end' className='w-[180px]'>
        <DropdownMenuLabel>{dag.dag_id}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => openDrawer(dag)}>
          Edit DAG
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
