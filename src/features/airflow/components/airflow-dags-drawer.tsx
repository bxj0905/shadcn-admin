import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  type Edge,
  type Node,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useQuery } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useAirflowDags } from './airflow-dags-provider'
import { fetchAirflowDagSource } from '@/services/airflow'

const initialNodes: Node[] = [
  { id: 'start', position: { x: 0, y: 0 }, data: { label: 'start' } },
  { id: 'task1', position: { x: 150, y: 0 }, data: { label: 'task_1' } },
  { id: 'task2', position: { x: 300, y: 0 }, data: { label: 'task_2' } },
]

const initialEdges: Edge[] = [
  { id: 'e1', source: 'start', target: 'task1' },
  { id: 'e2', source: 'task1', target: 'task2' },
]

export function AirflowDagsDrawer() {
  const { open, closeDrawer, selectedDag } = useAirflowDags()

  const dagId = selectedDag?.dag_id

  const { data: dagSource, isLoading, isError } = useQuery({
    queryKey: ['airflow-dag-source', dagId],
    queryFn: () => fetchAirflowDagSource(dagId as string),
    enabled: !!dagId,
  })

  return (
    <Dialog
      open={open}
      onOpenChange={(openValue) => {
        if (!openValue) closeDrawer()
      }}
    >
      <DialogContent
        className='flex h-[90vh] max-h-[90vh] w-[98vw] max-w-[1400px] sm:max-w-[98vw] lg:max-w-[1400px] flex-col gap-4 p-0'
        showCloseButton
      >
        <DialogHeader className='border-b px-6 py-4'>
          <DialogTitle className='text-lg font-semibold'>
            Edit DAG{selectedDag ? ` - ${selectedDag.dag_id}` : ''}
          </DialogTitle>
          <DialogDescription>
            Preview DAG code and a simple flow graph (mocked for now).
          </DialogDescription>
        </DialogHeader>

        <div className='flex flex-1 flex-col gap-4 px-6 pb-6 pt-2'>
          {/* Flow 图区域 */}
          <div className='h-[30vh] min-h-[200px] rounded-md border bg-muted/40'>
            <ReactFlow nodes={initialNodes} edges={initialEdges} fitView>
              <MiniMap zoomable pannable />
              <Controls />
              <Background gap={12} size={1} />
            </ReactFlow>
          </div>

          {/* 代码区域 */}
          <ScrollArea className='flex-1 min-h-[260px] rounded-md border bg-muted/40'>
            <div className='h-full w-full overflow-auto p-4 font-mono text-xs'>
              <pre className='whitespace-pre-wrap break-all'>
                {(() => {
                  if (!dagId) {
                    return '# Select a DAG row and click "Edit DAG" to preview its code.'
                  }
                  if (isLoading) {
                    return `# Loading DAG source for "${dagId}"...`
                  }
                  if (isError) {
                    return `# Failed to load DAG source for "${dagId}"`
                  }
                  if (dagSource?.source_code) {
                    return dagSource.source_code
                  }
                  return `# No source code returned for "${dagId}"`
                })()}
              </pre>
            </div>
          </ScrollArea>
        </div>
      </DialogContent>
    </Dialog>
  )
}
