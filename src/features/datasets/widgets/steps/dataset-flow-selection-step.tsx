import React from 'react'
import { Label } from '@/components/ui/label'
import { Card, CardContent } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { type PrefectFlow } from '@/services/prefect'

type DatasetFlowSelectionStepProps = {
  flows: PrefectFlow[]
  isLoading: boolean
  isError: boolean
  selectedFlowId: string | null
  runFlowMessage: string
  runFlowError: string
  onFlowSelect: (flowId: string) => void
}

export function DatasetFlowSelectionStep({
  flows,
  isLoading,
  isError,
  selectedFlowId,
  runFlowMessage,
  runFlowError,
  onFlowSelect,
}: DatasetFlowSelectionStepProps) {
  return (
    <div className='space-y-6'>
      <div>
        <Label className='text-base font-semibold'>选择处理模式</Label>
        <p className='text-muted-foreground mt-1 text-sm'>
          从 Prefect 中选择一个 Flow 作为该数据集的处理模型，选择后将触发 Flow 运行。
        </p>
      </div>

      <Card>
        <CardContent className='space-y-4 py-4'>
          {isLoading && (
            <p className='text-xs text-muted-foreground'>正在加载 Prefect flows...</p>
          )}
          {isError && (
            <p className='text-xs text-red-600'>加载 Prefect flows 失败，请稍后重试。</p>
          )}
          {!isLoading && !isError && flows.length === 0 && (
            <p className='text-xs text-muted-foreground'>
              当前暂无可用的 Prefect Flow，请先在「Prefect」页面创建或配置 Flow。
            </p>
          )}

          {!isLoading && !isError && flows.length > 0 && (
            <div className='grid gap-3 md:grid-cols-2'>
              {flows.map((flow) => {
                const isActive = selectedFlowId === flow.id
                const initials = flow.name.trim().slice(0, 2)
                return (
                  <button
                    key={flow.id}
                    type='button'
                    onClick={() => onFlowSelect(flow.id)}
                    className={cn(
                      'flex items-center gap-3 rounded-lg border p-3 text-left text-xs transition-colors',
                      isActive ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted/60',
                    )}
                  >
                    <div
                      className={cn(
                        'flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-md text-xs font-semibold',
                        isActive ? 'bg-emerald-50 text-emerald-700' : 'bg-muted text-muted-foreground',
                      )}
                    >
                      {initials}
                    </div>
                    <div className='flex-1'>
                      <div className='flex items-center justify-between gap-2'>
                        <span className='truncate text-sm font-medium'>{flow.name}</span>
                        {isActive && (
                          <span className='text-[11px] font-medium text-primary'>已选择</span>
                        )}
                      </div>
                      {flow.tags && flow.tags.length > 0 && (
                        <div className='mt-1 flex flex-wrap gap-1'>
                          {flow.tags.map((tag) => (
                            <span
                              key={tag}
                              className='rounded-full bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground'
                            >
                              {tag}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  </button>
                )
              })}
            </div>
          )}

          {runFlowMessage && (
            <p className='text-xs text-green-600'>
              {runFlowMessage}
            </p>
          )}
          {runFlowError && (
            <p className='text-xs text-red-600'>
              {runFlowError}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

