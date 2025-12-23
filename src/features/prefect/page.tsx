import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery } from '@tanstack/react-query'
import { ConfigDrawer } from '@/components/config-drawer'
import { Header } from '@/components/layout/header'
import { Main } from '@/components/layout/main'
import { ProfileDropdown } from '@/components/profile-dropdown'
import { Search } from '@/components/search'
import { ThemeSwitch } from '@/components/theme-switch'
import { Button } from '@/components/ui/button'
import { PrefectFlowsTable } from './components/prefect-flows-table'
import {
  fetchPrefectFlows,
  type PrefectFlow,
  type PrefectListFlowsResponse,
} from '@/services/prefect'
import { PrefectFlowNewDialog } from './components/prefect-flow-new-dialog'

export function PrefectFlowsPage() {
  const { t } = useTranslation()
  const [openCreate, setOpenCreate] = useState(false)

  const { data, isLoading, isError, error } = useQuery<
    PrefectListFlowsResponse,
    Error
  >({
    queryKey: ['prefect', 'flows'],
    queryFn: () => fetchPrefectFlows({ limit: 200, offset: 0 }),
  })

  if (isLoading) return <div>{t('prefect.loading')}</div>

  if (isError) {
    const message = error instanceof Error ? error.message : 'Unknown error'
    return <div>{t('prefect.error', { message })}</div>
  }

  const flows = (data?.flows ?? []) as PrefectFlow[]
  const total = data?.total ?? flows.length

  // 根据 labels 动态识别 flow 的层级关系
  // labels 中的 flow_type 和 parent_flow 字段用于标识关系
  const flowMap = new Map<string, PrefectFlow>()
  const parentChildMap = new Map<string, string[]>() // parentFlowId -> childFlowIds

  // 第一步：建立 flow 映射和父子关系
  flows.forEach((flow) => {
    flowMap.set(flow.id, flow)
    
    const flowType = flow.labels?.['flow_type'] as string | undefined
    const parentFlowName = flow.labels?.['parent_flow'] as string | undefined
    
    if (flowType === 'subflow' || flowType === 'feature') {
      // 查找父 Flow
      const parentFlow = flows.find((f) => f.name === parentFlowName)
      if (parentFlow) {
        if (!parentChildMap.has(parentFlow.id)) {
          parentChildMap.set(parentFlow.id, [])
        }
        parentChildMap.get(parentFlow.id)!.push(flow.id)
      }
    }
  })

  // 第二步：重新排序 flows，将子 Flow 放在父 Flow 之后
  const processedFlowIds = new Set<string>()
  const uiFlows: PrefectFlow[] = []

  // 先处理主 Flow（flow_type === 'main'）
    flows.forEach((flow) => {
    const flowType = flow.labels?.['flow_type'] as string | undefined
    if (flowType === 'main' && !processedFlowIds.has(flow.id)) {
      processedFlowIds.add(flow.id)
      uiFlows.push(flow)
      
      // 添加该主 Flow 的所有子 Flow
      const childFlowIds = parentChildMap.get(flow.id) || []
      childFlowIds.forEach((childId) => {
        if (!processedFlowIds.has(childId)) {
          const childFlow = flowMap.get(childId)
          if (childFlow) {
            processedFlowIds.add(childId)
            uiFlows.push({
              ...childFlow,
          isSubflow: true,
              parentFlowId: flow.id,
        })
          }
        }
      })
    }
  })

  // 然后处理其他 Flow（没有 parent_flow 或 parent_flow 不存在的）
  flows.forEach((flow) => {
    if (!processedFlowIds.has(flow.id)) {
      processedFlowIds.add(flow.id)
      uiFlows.push(flow)
      
      // 添加该 Flow 的所有子 Flow
      const childFlowIds = parentChildMap.get(flow.id) || []
      childFlowIds.forEach((childId) => {
        if (!processedFlowIds.has(childId)) {
          const childFlow = flowMap.get(childId)
          if (childFlow) {
            processedFlowIds.add(childId)
            uiFlows.push({
              ...childFlow,
              isSubflow: true,
              parentFlowId: flow.id,
            })
          }
        }
      })
    }
  })

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
            <h2 className='text-2xl font-bold tracking-tight'>
              {t('prefect.header.title')}
            </h2>
            <p className='text-muted-foreground'>{t('prefect.header.desc')}</p>
          </div>
          <div className='flex items-center gap-2'>
            <Button size='sm' onClick={() => setOpenCreate(true)}>
              新建工作流
            </Button>
          </div>
        </div>

        <div className='flex flex-1 flex-col gap-4'>
          <div className='flex items-center justify-between text-sm text-muted-foreground'>
            <div>
              {t('prefect.stats.showing', { count: uiFlows.length, total })}
            </div>
          </div>

          <PrefectFlowsTable data={uiFlows} />
        </div>
      </Main>
      <PrefectFlowNewDialog open={openCreate} onOpenChange={setOpenCreate} />
    </>
  )
}
